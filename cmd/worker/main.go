// Command worker runs a worker: a bounded pool that consumes jobs, runs their
// handlers, retries/dead-letters failures, and recovers stranded jobs (reaper).
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/demo"
	"github.com/aryan-bhokare/distributed-job-queue/internal/events"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
	"github.com/aryan-bhokare/distributed-job-queue/internal/scheduler"
	"github.com/aryan-bhokare/distributed-job-queue/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	rdb := redis.NewClient(&redis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()

	b := broker.New(rdb)
	pub := events.NewPublisher(rdb)
	w := worker.New(envOr("WORKER_NAME", "worker-1"), job.DefaultQueue, b, pub)
	demo.Register(w)

	if n, err := strconv.Atoi(os.Getenv("WORKER_CONCURRENCY")); err == nil {
		w.SetConcurrency(n)
	}
	// REAPER_MIN_IDLE must exceed the longest job duration (default 15s in New).
	if d, err := time.ParseDuration(os.Getenv("REAPER_MIN_IDLE")); err == nil {
		w.SetReaper(d, 0)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// The scheduler promotes due retries/delayed jobs. Safe to run in each worker
	// (the Lua move is atomic); a large deployment would run a dedicated one.
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
