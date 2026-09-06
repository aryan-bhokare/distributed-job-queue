// Command worker runs a single worker that consumes jobs and executes their
// registered handlers. Phase 1: one worker, one loop, no retries yet.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/events"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
	"github.com/aryan-bhokare/distributed-job-queue/internal/scheduler"
	"github.com/aryan-bhokare/distributed-job-queue/internal/worker"
)

func main() {
	// Structured JSON logs from day one — observability is not an afterthought.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	rdb := redis.NewClient(&redis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()

	b := broker.New(rdb)
	pub := events.NewPublisher(rdb) // publishes transitions to the dashboard bus
	w := worker.New(envOr("WORKER_NAME", "worker-1"), job.DefaultQueue, b, pub)
	if n, err := strconv.Atoi(os.Getenv("WORKER_CONCURRENCY")); err == nil {
		w.SetConcurrency(n)
	}

	// Demo handler #1: instant "email send" — the clean happy path.
	w.Register("send_email", func(ctx context.Context, j job.Job) error {
		var p struct {
			To       string `json:"to"`
			Template string `json:"template"`
		}
		_ = json.Unmarshal(j.Payload, &p)
		slog.Info("📧 sending email", "to", p.To, "template", p.Template)
		return nil
	})

	// Demo handler #2: a slower job that mimics PDF/LLM work (ties to Kyper/Foozi).
	w.Register("generate_pdf", func(ctx context.Context, j job.Job) error {
		slog.Info("📄 generating pdf...", "job_id", j.ID)
		time.Sleep(1500 * time.Millisecond)
		slog.Info("📄 pdf ready", "job_id", j.ID)
		return nil
	})

	// Demo handler #3: flaky — fails the first two attempts, then succeeds. Shows
	// retry-with-backoff recovering on its own.
	w.Register("flaky", func(ctx context.Context, j job.Job) error {
		if j.Attempt < 2 {
			return fmt.Errorf("transient failure on attempt %d", j.Attempt)
		}
		slog.Info("✅ flaky job finally succeeded", "job_id", j.ID, "attempt", j.Attempt)
		return nil
	})

	// Demo handler #4: always fails — shows the job exhausting retries and landing
	// in the dead-letter queue.
	w.Register("always_fail", func(ctx context.Context, j job.Job) error {
		return fmt.Errorf("permanent failure (attempt %d)", j.Attempt)
	})

	// signal.NotifyContext cancels ctx on Ctrl-C / SIGTERM; the worker drains
	// in-flight jobs (Phase 3) before returning.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The scheduler promotes due retries/delayed jobs back into their streams.
	// Running it inside each worker is safe (the Lua move is atomic); in a large
	// deployment you'd run a dedicated scheduler (or elect a leader).
	go scheduler.New(b).Run(ctx)

	if err := w.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("worker crashed", "err", err)
		os.Exit(1)
	}
	slog.Info("worker stopped")
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
