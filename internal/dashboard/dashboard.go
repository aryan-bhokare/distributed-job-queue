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
	"github.com/aryan-bhokare/distributed-job-queue/web"
)

const maxJobs = 200 // cap the in-memory view so it never grows unbounded

// jobView is a job's current state, derived by folding events together.
type jobView struct {
	ID         string    `json:"id"`
	Type       string    `json:"type"`
	State      string    `json:"state"` // queued|running|done|failed|skipped
	Worker     string    `json:"worker,omitempty"`
	Attempt    int       `json:"attempt"`
	Error      string    `json:"error,omitempty"`
	DurationMs int64     `json:"duration_ms,omitempty"`
	Updated    time.Time `json:"updated"`
}

type Server struct {
	rdb    *redis.Client
	broker *broker.Broker
	pub    *events.Publisher
	log    *slog.Logger

	mu      sync.Mutex
	clients map[chan []byte]struct{} // one channel per connected browser
	jobs    map[string]*jobView
	order   []string // insertion order of job IDs, for snapshots + capping
}

func NewServer(rdb *redis.Client) *Server {
	return &Server{
		rdb:     rdb,
		broker:  broker.New(rdb),
		pub:     events.NewPublisher(rdb),
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
	case events.Started:
		v.State = "running"
	case events.Succeeded:
		v.State, v.DurationMs = "done", e.DurationMs
	case events.Failed:
		v.State, v.Error, v.DurationMs = "failed", e.Error, e.DurationMs
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
	var typ string
	var payload any
	if time.Now().UnixNano()%2 == 0 {
		typ, payload = "send_email", map[string]any{"to": "demo@user.com", "template": "welcome"}
	} else {
		typ, payload = "generate_pdf", map[string]any{"doc": "invoice"}
	}
	j, err := job.New(typ, payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err := s.broker.Enqueue(r.Context(), j); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.pub.Publish(r.Context(), events.Event{Kind: events.Enqueued, JobID: j.ID, JobType: j.Type, Queue: j.Queue})
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
