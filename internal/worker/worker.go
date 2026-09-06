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
	"sync"
	"time"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/events"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
)

// Handler executes one job. Returning an error means the job failed. Payload is
// the job's raw JSON — the handler unmarshals whatever shape it expects.
type Handler func(ctx context.Context, j job.Job) error

type Worker struct {
	name        string
	queue       string
	broker      *broker.Broker
	pub         *events.Publisher // may be nil (worker runs fine without a dashboard)
	handlers    map[string]Handler
	concurrency int           // number of jobs processed in parallel
	grace       time.Duration // how long in-flight jobs get to finish on shutdown
	log         *slog.Logger
}

func New(name, queue string, b *broker.Broker, pub *events.Publisher) *Worker {
	return &Worker{
		name:        name,
		queue:       queue,
		broker:      b,
		pub:         pub,
		handlers:    make(map[string]Handler),
		concurrency: 5,
		grace:       25 * time.Second,
		log:         slog.Default().With("worker", name),
	}
}

// SetConcurrency sets how many jobs run in parallel (default 5).
func (w *Worker) SetConcurrency(n int) {
	if n > 0 {
		w.concurrency = n
	}
}

// SetShutdownGrace sets how long in-flight jobs may run after a stop signal.
func (w *Worker) SetShutdownGrace(d time.Duration) {
	if d > 0 {
		w.grace = d
	}
}

// Register wires a job type to the function that runs it.
func (w *Worker) Register(jobType string, h Handler) { w.handlers[jobType] = h }

// Run starts a pool of `concurrency` processor goroutines fed by a single fetcher,
// and returns after a graceful drain when ctx is cancelled (Ctrl-C / SIGTERM).
//
// The shape is the classic Go worker pool: a fetcher pushes jobs onto an unbuffered
// channel; N goroutines `range` over it. Closing the channel is the drain signal —
// each goroutine finishes its current job, sees the closed+empty channel, and exits.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.broker.EnsureGroup(ctx, w.queue); err != nil {
		return err
	}
	w.log.Info("worker started", "queue", w.queue, "concurrency", w.concurrency)

	jobs := make(chan broker.Delivered) // unbuffered → natural backpressure
	var wg sync.WaitGroup

	// procCtx drives handlers + acks and is deliberately NOT derived from ctx: a
	// SIGTERM cancels ctx, but we do NOT want to kill in-flight jobs or their acks
	// mid-flight (that's the "ack failed: context canceled" bug from Phase 2). We
	// drain within a grace period, then cancel only the stragglers.
	procCtx, procCancel := context.WithCancel(context.Background())
	defer procCancel()

	for i := 0; i < w.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for d := range jobs { // exits when jobs is closed AND drained
				w.process(procCtx, d)
			}
		}()
	}

	// Fetch until ctx is cancelled; the unbuffered channel means we only pull as
	// fast as the pool drains.
	w.fetch(ctx, jobs)

	// --- graceful shutdown ---
	w.log.Info("stop signal received; draining in-flight jobs", "grace", w.grace.String())
	close(jobs) // no more work; processors finish current jobs then return

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		w.log.Info("all in-flight jobs drained cleanly ✓")
	case <-time.After(w.grace):
		// Stragglers exceeded the grace window. Cancel them; because we never
		// ack'd them they stay in the PEL and get reclaimed by the reaper (Phase 6).
		w.log.Warn("grace elapsed; cancelling stragglers (they stay pending and get reclaimed)")
		procCancel()
		wg.Wait()
	}
	return nil
}

// fetch reads batches from the broker and feeds the pool until ctx is cancelled.
func (w *Worker) fetch(ctx context.Context, jobs chan<- broker.Delivered) {
	for ctx.Err() == nil {
		batch, err := w.broker.Read(ctx, w.queue, w.name, int64(w.concurrency), 2*time.Second)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.log.Error("read failed", "err", err)
			time.Sleep(time.Second)
			continue
		}
		for _, d := range batch {
			select {
			case jobs <- d:
			case <-ctx.Done():
				return // stop feeding; delivered-but-unfed entries remain in the PEL
			}
		}
	}
}

func (w *Worker) process(ctx context.Context, d broker.Delivered) {
	start := time.Now()
	log := w.log.With("job_id", d.Job.ID, "type", d.Job.Type)

	// base event carries the fields common to every transition for this job.
	base := events.Event{JobID: d.Job.ID, JobType: d.Job.Type, Queue: d.Job.Queue,
		Worker: w.name, Attempt: d.Job.Attempt}

	h, ok := w.handlers[d.Job.Type]
	if !ok {
		// No handler for this type. Ack so it doesn't sit in the PEL forever.
		// (Phase 4 will route unknown/failed jobs to the DLQ instead.)
		log.Warn("no handler registered; acking and skipping")
		w.emit(ctx, base, events.NoHandler)
		_ = w.broker.Ack(ctx, w.queue, d.EntryID)
		return
	}

	w.emit(ctx, base, events.Started)
	err := h(ctx, d.Job)
	dur := time.Since(start)

	if err != nil {
		// Phase 1: log and ack to avoid an infinite redelivery loop.
		// Phase 4 replaces this with retry-with-backoff, then DLQ.
		log.Error("job failed (acking for now; retries land in Phase 4)", "err", err)
		ev := base
		ev.Error = err.Error()
		ev.DurationMs = dur.Milliseconds()
		w.emit(ctx, ev, events.Failed)
		_ = w.broker.Ack(ctx, w.queue, d.EntryID)
		return
	}

	// Only ack AFTER the handler succeeds — this is the crux of at-least-once.
	if err := w.broker.Ack(ctx, w.queue, d.EntryID); err != nil {
		log.Error("ack failed (job will be redelivered)", "err", err)
		return
	}
	ev := base
	ev.DurationMs = dur.Milliseconds()
	w.emit(ctx, ev, events.Succeeded)
	log.Info("job done", "dur", dur.String())
}

// emit publishes a transition to the event bus (no-op if no publisher wired).
func (w *Worker) emit(ctx context.Context, e events.Event, kind string) {
	e.Kind = kind
	w.pub.Publish(ctx, e)
}
