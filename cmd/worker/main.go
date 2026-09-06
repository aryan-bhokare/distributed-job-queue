// Command worker runs a single worker that consumes jobs and executes their
// registered handlers. Phase 1: one worker, one loop, no retries yet.
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/events"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
	"github.com/aryan-bhokare/distributed-job-queue/internal/worker"
)

func main() {
	// Structured JSON logs from day one — observability is not an afterthought.
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	rdb := redis.NewClient(&redis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()

	b := broker.New(rdb)
	pub := events.NewPublisher(rdb) // publishes transitions to the dashboard bus
	w := worker.New("worker-1", job.DefaultQueue, b, pub)

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

	// signal.NotifyContext cancels ctx on Ctrl-C / SIGTERM; the worker loop then
	// returns. Full graceful drain of in-flight jobs lands in Phase 3.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
