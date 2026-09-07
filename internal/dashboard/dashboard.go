// Package dashboard is the recruiter-facing surface. It subscribes to the job
// event bus, keeps a small in-memory view of each job's current state, and
// streams updates to browsers over Server-Sent Events (SSE). It also serves the
// embedded web UI and a minimal enqueue endpoint so the demo is self-contained.
//
// Design: the dashboard is a pure OBSERVER of the bus — it never touches the job
// hot path. See docs/adr/0003.
package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/events"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
	"github.com/aryan-bhokare/distributed-job-queue/pkg/jobqueue"
	"github.com/aryan-bhokare/distributed-job-queue/web"
)

const maxJobs = 200 // cap the in-memory view so it never grows unbounded

// jobView is a job's current state, derived by folding events together.
type jobView struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	State      string    `json:"state"` // queued|running|retrying|done|dead|skipped
	Worker     string    `json:"worker,omitempty"`
	Attempt    int       `json:"attempt"`
	Error      string    `json:"error,omitempty"`
	DurationMs int64     `json:"duration_ms,omitempty"`
	RetryInMs  int64     `json:"retry_in_ms,omitempty"`
	Updated    time.Time `json:"updated"`
}

type Server struct {
	rdb    *redis.Client
	broker *broker.Broker   // for health checks
	client *jobqueue.Client // for the enqueue control endpoint
	log    *slog.Logger

	// Optional worker-process controls, wired by cmd/demo so the browser can
	// spawn/kill real worker processes (for the reaper demo). Nil = disabled.
	addWorker   func() (string, error)
	killWorker  func() (string, error)
	workerCount func() int

	mu      sync.Mutex
	clients map[chan []byte]struct{} // one channel per connected browser
	jobs    map[string]*jobView
	order   []string // insertion order of job IDs, for snapshots + capping
}

func NewServer(rdb *redis.Client) *Server {
	return &Server{
		rdb:     rdb,
		broker:  broker.New(rdb),
		client:  jobqueue.New(rdb),
		log:     slog.Default().With("svc", "dashboard"),
		clients: make(map[chan []byte]struct{}),
		jobs:    make(map[string]*jobView),
	}
}

// Run starts the event pump and serves HTTP until ctx is cancelled.
func (s *Server) Run(ctx context.Context, addr string) error {
	go s.pump(ctx) // bus -> state + broadcast

	mux := http.NewServeMux()
	mux.HandleFunc("GET /events", s.handleSSE)
	mux.HandleFunc("POST /api/enqueue", s.handleEnqueue)
	mux.HandleFunc("POST /api/worker/add", s.handleAddWorker)
	mux.HandleFunc("POST /api/worker/kill", s.handleKillWorker)
	mux.HandleFunc("GET /api/workers", s.handleWorkers)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /", http.FileServerFS(web.FS)) // "/" -> index.html, plus app.js/styles.css

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() { <-ctx.Done(); _ = srv.Shutdown(context.Background()) }()

	s.log.Info("dashboard listening", "addr", "http://"+addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// pump reads the event bus, updates the in-memory view, and broadcasts each raw
// event JSON to every connected browser.
func (s *Server) pump(ctx context.Context) {
	for e := range events.Subscribe(ctx, s.rdb) {
		s.apply(e)
		if data, err := json.Marshal(e); err == nil {
			s.broadcast(data)
		}
	}
}

// apply folds one event into the job's current state.
func (s *Server) apply(e events.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.jobs[e.JobID]
	if !ok {
		v = &jobView{ID: e.JobID, Type: e.JobType}
		s.jobs[e.JobID] = v
		s.order = append(s.order, e.JobID)
		if len(s.order) > maxJobs { // evict oldest
			oldest := s.order[0]
			s.order = s.order[1:]
			delete(s.jobs, oldest)
		}
	}
	if e.JobType != "" {
		v.Type = e.JobType
	}
	v.Worker = e.Worker
	v.Attempt = e.Attempt
	v.Updated = e.At
	switch e.Kind {
	case events.Enqueued:
		v.State = "queued"
		v.RetryInMs = 0
	case events.Started:
		v.State = "running"
	case events.Succeeded:
		v.State, v.DurationMs = "done", e.DurationMs
	case events.Retrying:
		v.State, v.Error, v.DurationMs, v.RetryInMs = "retrying", e.Error, e.DurationMs, e.RetryInMs
	case events.Dead:
		v.State, v.Error, v.DurationMs = "dead", e.Error, e.DurationMs
	case events.Reclaimed:
		// Recovered from a crashed worker; it's about to be reprocessed here.
		v.State, v.Worker = "running", e.Worker
	case events.NoHandler:
		v.State = "skipped"
	}
}

func (s *Server) broadcast(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.clients {
		select {
		case ch <- data:
		default: // slow browser: drop this update for it rather than block everyone
		}
	}
}

// handleSSE streams events to one browser. On connect it sends a snapshot of the
// current job view so a late joiner isn't blank, then tails live updates.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan []byte, 64)
	s.mu.Lock()
	s.clients[ch] = struct{}{}
	snap := make([]*jobView, 0, len(s.order))
	for _, id := range s.order {
		snap = append(snap, s.jobs[id])
	}
	s.mu.Unlock()

	// Named "snapshot" event seeds the board; live updates arrive as default messages.
	if b, err := json.Marshal(snap); err == nil {
		fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", b)
		flusher.Flush()
	}

	defer func() {
		s.mu.Lock()
		delete(s.clients, ch)
		close(ch)
		s.mu.Unlock()
	}()

	ping := time.NewTicker(20 * time.Second) // comment-ping keeps the connection warm
	defer ping.Stop()

	for {
		select {
		case <-r.Context().Done(): // browser closed the tab
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		case data := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

// handleEnqueue enqueues one random demo job so the dashboard is self-contained
// (click a button, watch it flow). Kill-worker / fail / delay controls land in
// Phase 7.
func (s *Server) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	typ := q.Get("type") // optional: send_email|generate_pdf|flaky|always_fail
	var payload any = map[string]any{}
	switch typ {
	case "":
		// no type given → a random happy-path job
		if time.Now().UnixNano()%2 == 0 {
			typ, payload = "send_email", map[string]any{"to": "demo@user.com", "template": "welcome"}
		} else {
			typ, payload = "generate_pdf", map[string]any{"doc": "invoice"}
		}
	case "send_email":
		payload = map[string]any{"to": "demo@user.com", "template": "welcome"}
	case "generate_pdf", "slow":
		payload = map[string]any{"doc": "invoice"}
	}

	var opts []jobqueue.Option
	if typ == "always_fail" {
		opts = append(opts, jobqueue.WithMaxRetries(3)) // reach the DLQ quickly in the demo
	}

	var (
		j   job.Job
		err error
	)
	if delay, derr := time.ParseDuration(q.Get("delay")); derr == nil && delay > 0 {
		j, err = s.client.EnqueueIn(r.Context(), delay, typ, payload, opts...)
	} else {
		j, err = s.client.Enqueue(r.Context(), typ, payload, opts...)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"id": j.ID, "type": j.Type})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := s.broker.Ping(r.Context()); err != nil {
		http.Error(w, "redis unreachable", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready"))
}

// SetWorkerControls wires functions that spawn / kill / count real worker
// processes so the dashboard can offer "Add worker" and "Kill worker" (cmd/demo).
func (s *Server) SetWorkerControls(add, kill func() (string, error), count func() int) {
	s.addWorker, s.killWorker, s.workerCount = add, kill, count
}

func (s *Server) handleWorkers(w http.ResponseWriter, _ *http.Request) {
	enabled := s.workerCount != nil
	n := 0
	if enabled {
		n = s.workerCount()
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"count": n, "enabled": enabled})
}

func (s *Server) handleAddWorker(w http.ResponseWriter, _ *http.Request) {
	if s.addWorker == nil {
		http.Error(w, "worker controls not enabled (run cmd/demo)", http.StatusNotImplemented)
		return
	}
	name, err := s.addWorker()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.log.Info("spawned worker", "worker", name)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"worker": name, "action": "added"})
}

func (s *Server) handleKillWorker(w http.ResponseWriter, _ *http.Request) {
	if s.killWorker == nil {
		http.Error(w, "worker controls not enabled (run cmd/demo)", http.StatusNotImplemented)
		return
	}
	name, err := s.killWorker()
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	s.log.Warn("killed worker (SIGKILL)", "worker", name)
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"worker": name, "action": "killed"})
}
