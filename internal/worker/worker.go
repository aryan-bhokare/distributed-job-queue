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
	"math/rand/v2"
	"sync"
	"time"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/events"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
)

// Backoff tuning. Small base for a snappy demo; raise base in production.
const (
	baseBackoff = 700 * time.Millisecond
	maxBackoff  = 8 * time.Second
)

// Handler executes one job. Returning an error means the job failed. Payload is
// the job's raw JSON — the handler unmarshals whatever shape it expects.
type Handler func(ctx context.Context, j job.Job) error

type Worker struct {
	name        string
	queue       string
	broker      *broker.Broker
	pub         *events.Publisher // may be nil (worker runs fine without a dashboard)
	handlers     map[string]Handler
	concurrency  int           // number of jobs processed in parallel
	grace        time.Duration // how long in-flight jobs get to finish on shutdown
	reapMinIdle  time.Duration // reclaim PEL entries idle longer than this (0 = off)
	reapInterval time.Duration // how often the reaper scans the PEL
	log          *slog.Logger
}

func New(name, queue string, b *broker.Broker, pub *events.Publisher) *Worker {
	return &Worker{
		name:         name,
		queue:        queue,
		broker:       b,
		pub:          pub,
		handlers:     make(map[string]Handler),
		concurrency:  5,
		grace:        25 * time.Second,
		reapMinIdle:  15 * time.Second, // must exceed the longest expected job duration
		reapInterval: 5 * time.Second,
		log:          slog.Default().With("worker", name),
	}
}

// SetReaper tunes dead-worker recovery. minIdle must exceed the longest job
// duration; pass minIdle <= 0 to disable reaping on this worker.
func (w *Worker) SetReaper(minIdle, interval time.Duration) {
	w.reapMinIdle = minIdle
	if interval > 0 {
		w.reapInterval = interval
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

	// Two producers feed the pool: the fetcher (new jobs) and the reaper
	// (jobs reclaimed from crashed workers). Both stop when ctx is cancelled;
	// we wait for BOTH before closing `jobs`, so neither ever sends on a closed
	// channel.
	var producers sync.WaitGroup
	producers.Add(2)
	go func() { defer producers.Done(); w.fetch(ctx, jobs) }()
	go func() { defer producers.Done(); w.reap(ctx, jobs) }()
	producers.Wait()

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

// reap periodically reclaims jobs stranded in the PEL by crashed workers (via
// XAUTOCLAIM) and feeds them back into the pool for reprocessing. This is our
// auto-recovery: if a worker dies mid-job without acking, another worker's reaper
// picks the job up after reapMinIdle. Disabled when reapMinIdle <= 0.
func (w *Worker) reap(ctx context.Context, jobs chan<- broker.Delivered) {
	if w.reapMinIdle <= 0 {
		return
	}
	ticker := time.NewTicker(w.reapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			claimed, err := w.broker.Reap(ctx, w.queue, w.name, w.reapMinIdle, int64(w.concurrency))
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				w.log.Error("reap failed", "err", err)
				continue
			}
			for _, d := range claimed {
				w.log.Warn("♻️  reclaimed stranded job from the PEL", "job_id", d.Job.ID, "type", d.Job.Type)
				w.emit(ctx, events.Event{JobID: d.Job.ID, JobType: d.Job.Type, Queue: d.Job.Queue,
					Worker: w.name, Attempt: d.Job.Attempt}, events.Reclaimed)
				select {
				case jobs <- d:
				case <-ctx.Done():
					return
				}
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
		w.handleFailure(ctx, d, base, err, dur)
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

// handleFailure decides what happens to a job whose handler returned an error:
// retry it with backoff if attempts remain, otherwise dead-letter it. In both
// cases we ack the current stream entry — the job's future now lives in the
// scheduled set (retry) or the DLQ (dead), so leaving it in the PEL would be a
// duplicate. On a *scheduling/DLQ* failure we deliberately DON'T ack, so the job
// stays pending and the reaper can recover it.
func (w *Worker) handleFailure(ctx context.Context, d broker.Delivered, base events.Event, cause error, dur time.Duration) {
	log := w.log.With("job_id", d.Job.ID, "type", d.Job.Type)

	if d.Job.Attempt < d.Job.MaxRetries {
		next := d.Job
		next.Attempt++
		delay := backoff(next.Attempt)
		if err := w.broker.Schedule(ctx, next, time.Now().Add(delay)); err != nil {
			log.Error("could not schedule retry; leaving in PEL for the reaper", "err", err)
			return
		}
		ev := base
		ev.Attempt = next.Attempt
		ev.Error = cause.Error()
		ev.DurationMs = dur.Milliseconds()
		ev.RetryInMs = delay.Milliseconds()
		w.emit(ctx, ev, events.Retrying)
		log.Warn("job failed; retry scheduled", "attempt", next.Attempt, "retry_in", delay.String(), "err", cause)
		_ = w.broker.Ack(ctx, w.queue, d.EntryID)
		return
	}

	// Retries exhausted → dead-letter queue.
	if err := w.broker.PushDead(ctx, d.Job, cause.Error()); err != nil {
		log.Error("could not push to DLQ; leaving in PEL for the reaper", "err", err)
		return
	}
	ev := base
	ev.Error = cause.Error()
	ev.DurationMs = dur.Milliseconds()
	w.emit(ctx, ev, events.Dead)
	log.Error("job dead-lettered (retries exhausted)", "attempts", d.Job.Attempt+1, "err", cause)
	_ = w.broker.Ack(ctx, w.queue, d.EntryID)
}

// backoff returns the delay before retry attempt n (n>=1): an exponential base
// (base * 2^(n-1), capped at maxBackoff) with "equal jitter" — half fixed, half
// random — so many jobs failing at once retry at spread-out times instead of
// firing in a synchronized thundering herd.
func backoff(n int) time.Duration {
	exp := baseBackoff << (n - 1) // base * 2^(n-1)
	if exp <= 0 || exp > maxBackoff {
		exp = maxBackoff // overflow or over-cap → clamp
	}
	half := exp / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
