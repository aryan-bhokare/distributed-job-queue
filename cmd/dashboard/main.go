// Command dashboard runs the live dashboard server: it subscribes to the job
// event bus and streams updates to browsers over SSE, serving the embedded UI.
//
//	go run ./cmd/dashboard      # then open http://localhost:8080
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/internal/dashboard"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	rdb := redis.NewClient(&redis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := dashboard.NewServer(rdb)
	if err := srv.Run(ctx, envOr("DASHBOARD_ADDR", ":8080")); err != nil {
		slog.Error("dashboard crashed", "err", err)
		os.Exit(1)
	}
	slog.Info("dashboard stopped")
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
