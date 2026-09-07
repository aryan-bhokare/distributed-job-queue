// Command demo runs the whole system as ONE process for the interactive
// dashboard: it serves the dashboard AND manages real worker subprocesses that
// the browser can spawn and kill — so a visitor can watch the reaper recover a
// job from a genuinely SIGKILL'd worker (nothing is faked).
//
//	go run ./cmd/demo    # then open http://localhost:8080
//
// It re-execs this same binary with DJQ_ROLE=worker to run each worker.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/dashboard"
	"github.com/aryan-bhokare/distributed-job-queue/internal/demo"
	"github.com/aryan-bhokare/distributed-job-queue/internal/events"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
	"github.com/aryan-bhokare/distributed-job-queue/internal/scheduler"
	"github.com/aryan-bhokare/distributed-job-queue/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	if os.Getenv("DJQ_ROLE") == "worker" {
		runWorker() // child process
		return
	}
	runDashboard() // parent process
}

// runWorker is the child role: a normal worker with the demo handlers. Reaper
// minIdle (8s) is set above the longest demo job (slow = 6s) so live work is
// never falsely reclaimed.
func runWorker() {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr()})
	defer rdb.Close()

	b := broker.New(rdb)
	w := worker.New(envOr("WORKER_NAME", "worker"), job.DefaultQueue, b, events.NewPublisher(rdb))
	w.SetConcurrency(3)
	w.SetReaper(8*time.Second, 2*time.Second)
	demo.Register(w)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go scheduler.New(b).Run(ctx)
	_ = w.Run(ctx)
}

// runDashboard serves the dashboard and manages worker subprocesses.
func runDashboard() {
	rdb := redis.NewClient(&redis.Options{Addr: redisAddr()})
	defer rdb.Close()

	mgr := newManager()
	defer mgr.killAll()

	srv := dashboard.NewServer(rdb)
	srv.SetWorkerControls(mgr.add, mgr.kill, mgr.count)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Start with two workers so the reaper demo works out of the box: kill one,
	// the other reclaims its jobs.
	for i := 0; i < 2; i++ {
		if _, err := mgr.add(); err != nil {
			slog.Error("could not spawn worker", "err", err)
		}
	}

	slog.Info("demo ready → open http://localhost:8080 (enqueue slow jobs, then Kill a worker)")
	if err := srv.Run(ctx, envOr("DASHBOARD_ADDR", ":8080")); err != nil {
		slog.Error("dashboard crashed", "err", err)
		os.Exit(1)
	}
}

// manager spawns/kills worker subprocesses by re-exec'ing this binary.
type manager struct {
	mu    sync.Mutex
	self  string
	n     int
	procs map[string]*exec.Cmd
}

func newManager() *manager {
	self, err := os.Executable()
	if err != nil {
		self = os.Args[0]
	}
	return &manager{self: self, procs: make(map[string]*exec.Cmd)}
}

func (m *manager) add() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n++
	name := fmt.Sprintf("worker-%d", m.n)
	cmd := exec.Command(m.self)
	cmd.Env = append(os.Environ(), "DJQ_ROLE=worker", "WORKER_NAME="+name)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}
	m.procs[name] = cmd
	go func() { _ = cmd.Wait(); m.forget(name) }() // avoid a zombie when it exits
	return name, nil
}

// kill SIGKILLs one worker — a hard crash, not a graceful stop, so its in-flight
// jobs are left un-acked in the PEL for the reaper to recover.
func (m *manager) kill() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, cmd := range m.procs {
		_ = cmd.Process.Kill()
		delete(m.procs, name)
		return name, nil
	}
	return "", errors.New("no workers to kill")
}

func (m *manager) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.procs)
}

func (m *manager) forget(name string) {
	m.mu.Lock()
	delete(m.procs, name)
	m.mu.Unlock()
}

func (m *manager) killAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, cmd := range m.procs {
		_ = cmd.Process.Kill()
	}
}

func redisAddr() string { return envOr("REDIS_ADDR", "localhost:6379") }

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
