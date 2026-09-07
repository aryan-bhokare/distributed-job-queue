// Package demo holds the sample job handlers used by cmd/worker and the
// interactive demo (cmd/demo). Kept in one place so both register the same set.
package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
	"github.com/aryan-bhokare/distributed-job-queue/internal/worker"
)

// Register wires the demo handlers onto a worker.
func Register(w *worker.Worker) {
	// Quick "email" — a short job so the happy path is visible but snappy.
	w.Register("send_email", func(_ context.Context, j job.Job) error {
		var p struct {
			To       string `json:"to"`
			Template string `json:"template"`
		}
		_ = json.Unmarshal(j.Payload, &p)
		slog.Info("📧 sending email", "to", p.To)
		time.Sleep(300 * time.Millisecond)
		return nil
	})

	// Slower "pdf/LLM" work — long enough to see it sit in Running (and to kill a
	// worker mid-flight for the reaper demo).
	w.Register("generate_pdf", func(_ context.Context, j job.Job) error {
		slog.Info("📄 generating pdf...", "job_id", j.ID)
		time.Sleep(1500 * time.Millisecond)
		slog.Info("📄 pdf ready", "job_id", j.ID)
		return nil
	})

	// Flaky — fails the first two attempts, then succeeds (retries recover it).
	w.Register("flaky", func(_ context.Context, j job.Job) error {
		if j.Attempt < 2 {
			return fmt.Errorf("transient failure on attempt %d", j.Attempt)
		}
		slog.Info("✅ flaky job finally succeeded", "job_id", j.ID, "attempt", j.Attempt)
		return nil
	})

	// Always fails — exhausts retries and lands in the dead-letter queue.
	w.Register("always_fail", func(_ context.Context, j job.Job) error {
		return fmt.Errorf("permanent failure (attempt %d)", j.Attempt)
	})

	// Long job (~6s) — stays in Running long enough to kill its worker mid-flight
	// and watch the reaper recover it.
	w.Register("slow", func(_ context.Context, j job.Job) error {
		slog.Info("🐢 slow job running...", "job_id", j.ID)
		time.Sleep(6 * time.Second)
		slog.Info("🐢 slow job done", "job_id", j.ID)
		return nil
	})
}
