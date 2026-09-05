// Package worker turns delivered jobs into executed work. It holds a registry of
// handlers keyed by job type and runs the read -> handle -> ack loop.
//
// Phase 1 scope: a single loop, no retries, no concurrency. Failures are logged
// and ack'd (so we never spin forever); retries/backoff/DLQ arrive in Phase 4,
// and the goroutine pool + graceful drain arrive in Phase 3.
package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
)

// Handler executes one job. Returning an error means the job failed. Payload is
// the job's raw JSON — the handler unmarshals whatever shape it expects.
type Handler func(ctx context.Context, j job.Job) error

type Worker struct {
	name     string
	queue    string
	broker   *broker.Broker
	handlers map[string]Handler
	log      *slog.Logger
}

func New(name, queue string, b *broker.Broker) *Worker {
	return &Worker{
		name:     name,
		queue:    queue,
		broker:   b,
		handlers: make(map[string]Handler),
		log:      slog.Default().With("worker", name),
	}
}

// Register wires a job type to the function that runs it.
func (w *Worker) Register(jobType string, h Handler) { w.handlers[jobType] = h }

// Run is the worker loop. It returns when ctx is cancelled (Ctrl-C / SIGTERM).
func (w *Worker) Run(ctx context.Context) error {
	if err := w.broker.EnsureGroup(ctx, w.queue); err != nil {
		return err
	}
	w.log.Info("worker started", "queue", w.queue)

	for {
		// Respect cancellation before doing more work.
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Block up to 5s for new jobs; the timeout lets us loop back and re-check
		// ctx even when the queue is idle.
		batch, err := w.broker.Read(ctx, w.queue, w.name, 10, 5*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			w.log.Error("read failed", "err", err)
			time.Sleep(time.Second) // brief backoff before retrying the read
			continue
		}
		for _, d := range batch {
			w.process(ctx, d)
		}
	}
}

func (w *Worker) process(ctx context.Context, d broker.Delivered) {
	start := time.Now()
	log := w.log.With("job_id", d.Job.ID, "type", d.Job.Type)

	h, ok := w.handlers[d.Job.Type]
	if !ok {
		// No handler for this type. Ack so it doesn't sit in the PEL forever.
		// (Phase 4 will route unknown/failed jobs to the DLQ instead.)
		log.Warn("no handler registered; acking and skipping")
		_ = w.broker.Ack(ctx, w.queue, d.EntryID)
		return
	}

	if err := h(ctx, d.Job); err != nil {
		// Phase 1: log and ack to avoid an infinite redelivery loop.
		// Phase 4 replaces this with retry-with-backoff, then DLQ.
		log.Error("job failed (acking for now; retries land in Phase 4)", "err", err)
		_ = w.broker.Ack(ctx, w.queue, d.EntryID)
		return
	}

	// Only ack AFTER the handler succeeds — this is the crux of at-least-once.
	if err := w.broker.Ack(ctx, w.queue, d.EntryID); err != nil {
		log.Error("ack failed (job will be redelivered)", "err", err)
		return
	}
	log.Info("job done", "dur", time.Since(start).String())
}
