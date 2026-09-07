# LEARNINGS — Distributed Job Queue

Running log of what I learned, gotchas, and "aha" moments. This is my study material — keep it
current as we go, not at the end.

## Phase 1 — vertical slice (2026-09-05)

**Redis Streams**
- `XADD jobs:default * ...` appends; `*` = server-assigned, time-ordered entry ID.
- `XGROUP CREATE ... MKSTREAM 0` creates the group (and stream) starting at the beginning. If the
  group already exists Redis returns a **BUSYGROUP** error — swallow it as "already set up".
- `XREADGROUP GROUP workers <consumer> ... STREAMS jobs:default >` — the `>` means "entries never
  delivered to this group". Each delivered entry is recorded in the **PEL** (Pending Entries List).
- **`XACK` only after the handler succeeds** — this single ordering is what makes delivery
  at-least-once: crash before ack ⇒ the entry is still pending ⇒ recoverable.
- **Aha:** Streams **retain** entries after ack — `XACK` removes from the PEL, not from the stream.
  `XLEN` stayed at 2 after both jobs finished; `XPENDING` was 0. Need `MAXLEN`/`XTRIM` later to cap.
- `redis.Nil` from `XReadGroup` = the block window elapsed with no new entries — normal, not an error.

**Go**
- Struct `json:"..."` tags = field naming on the wire (like Pydantic aliases).
- `any` == `interface{}`; used it so `job.New` accepts a struct or a map and marshals internally.
- Error wrapping with `fmt.Errorf("...: %w", err)` keeps the cause inspectable — Go's way of adding
  context without hiding the underlying error.
- `signal.NotifyContext` gives a `context.Context` that cancels on Ctrl-C/SIGTERM — clean shutdown
  signal; the worker loop just checks `ctx.Err()`.
- `log/slog` (stdlib) → structured JSON logs with zero deps: `slog.Info("msg", "k", v)`.
- One `Broker` type wraps all Redis calls, so the transport details live in exactly one file.

**Design choices made concrete**
- Phase 1 deliberately acks on handler failure to avoid an infinite redelivery loop; real
  retry/backoff/DLQ is Phase 4. Documented the shortcut in code comments so it's not mistaken for
  final behavior.

## Phase 2 — event bus + live dashboard (2026-09-06)

**Redis Pub/Sub**
- `PUBLISH job.events <json>` is fire-and-forget: if no one's subscribed, the message just vanishes
  (no history/replay). That's *why* the dashboard can never slow or break job processing.
- Because there's no replay, a browser that connects late would be blank — so the dashboard keeps a
  small in-memory view of each job and sends a **snapshot** on every new SSE connection.

**SSE (Server-Sent Events)**
- Wire format is dead simple: `data: <text>\n\n` per message; a named event is `event: name\n` then
  the `data:` line. A line starting with `:` is a comment (I use `: ping` as a keep-alive).
- Server side: set `Content-Type: text/event-stream`, write, then call `http.Flusher.Flush()` to
  push bytes immediately (otherwise Go buffers the response).
- Client side: `new EventSource('/events')` — auto-reconnects on drop for free. `addEventListener
  ('snapshot', …)` for the named event; `onmessage` for default `data:` messages.
- Chose SSE over WebSockets because the stream is one-way (server→browser); the browser's *actions*
  (enqueue) are just `fetch(..., {method:'POST'})`.

**Go**
- `//go:embed index.html app.js styles.css` + `var FS embed.FS` bakes the UI into the binary. embed
  only sees files in the *same directory*, so `web/` has its own `embed.go` (can't do `../web`).
- `http.FileServerFS(web.FS)` (Go 1.22+) serves the embedded files; `"/"` → `index.html`.
- ServeMux method patterns: `mux.HandleFunc("GET /events", …)` / `"POST /api/enqueue"` (Go 1.22+).
- SSE hub pattern: a `map[chan []byte]struct{}` of connected clients guarded by a mutex;
  `broadcast` does a non-blocking send (`select { case ch<-data: default: }`) so one slow browser
  can't stall the others.
- A nil `*events.Publisher` is a safe no-op (guard inside `Publish`) — so a worker runs fine with or
  without a dashboard wired in.

**Testing gotcha**
- macOS has no `timeout(1)`; use `curl --max-time N` to bound a streaming request in a test.

## Phase 3 — worker pool + graceful shutdown (2026-09-06)

**The Go worker-pool pattern**
- One unbuffered `chan Delivered`, a fetcher that pushes onto it, and N goroutines that `for d :=
  range jobs`. The unbuffered channel = **natural backpressure**: the fetcher blocks until a
  processor is free, so we never pull more than the pool can handle.
- **Closing the channel is the drain signal.** `range` over a channel exits once it's closed AND
  empty — so after `close(jobs)`, each goroutine finishes its current job, sees the drained channel,
  and returns. Clean, no sentinel values.
- `sync.WaitGroup` tracks the pool: `wg.Add(1)` per goroutine, `defer wg.Done()`, `wg.Wait()` to
  join. To wait-with-a-deadline, run `wg.Wait()` in a goroutine that closes a `done` channel, then
  `select { case <-done: case <-time.After(grace): }`.

**The two-context trick (the crux of graceful shutdown)**
- The root ctx (from `signal.NotifyContext`) cancels on SIGTERM. If handlers/acks used *that* ctx,
  they'd be killed the instant SIGTERM lands → the "ack failed: context canceled" bug.
- Fix: the **fetcher** uses the root ctx (so it stops pulling new work on SIGTERM), but the
  **processors** use a *separate* `procCtx` (from `context.Background()`) that survives SIGTERM.
  In-flight jobs finish and ack normally; only if they exceed the grace window do we `procCancel()`
  the stragglers (which then stay unacked in the PEL → reclaimed by the reaper).
- Proven: SIGTERM mid-job → the job ran to completion (`dur 1.5s`), acked, `XPENDING=0`, zero
  "ack failed". Contrast Phase 2 where the same kill produced a canceled ack.

**Distributed for free**
- Two `cmd/worker` processes with different `WORKER_NAME`s split a 20-job burst exactly 10/10 — the
  consumer group hands each *consumer* a disjoint set of entries. Horizontal scaling needs no code,
  just more processes. (Consumer name matters: it identifies the consumer in the group + its PEL.)

**Minor**
- go-redis's `XReadGroup` with `Block` doesn't abort instantly on ctx-cancel; it can wait out the
  block timeout (I use 2s), so shutdown can lag up to ~2s. Fine; documented.

## Phase 4 — retries + backoff + DLQ (2026-09-06)

**Retry model**
- On handler error: if `Attempt < MaxRetries`, bump `Attempt`, compute a backoff, and `Schedule`
  the job in a sorted set (`jobs:scheduled`, score = run-at ms) — then **ack the current entry**.
  The retry now lives in the scheduled set, so leaving the original in the PEL would double it.
- At `Attempt == MaxRetries`, `XADD` the job + error to `jobs:dead` (DLQ) and ack. A poison job
  never blocks the queue or loops forever.
- Crucial subtlety: if the *scheduling/DLQ write itself* fails, do NOT ack — leave it in the PEL so
  the reaper (Phase 6) recovers it. Ack only once the job's future is safely stored somewhere.

**Exponential backoff + jitter (why)**
- `delay = base * 2^(attempt-1)`, capped, with **equal jitter** (half fixed + half random). Without
  jitter, a batch of jobs that all fail at the same instant would all retry at the same instant —
  a synchronized "thundering herd" that re-overloads whatever just failed. Jitter spreads them out.
- Proven live: backoffs grew 517ms → 1.2s → 2.0s across attempts.

**The scheduler + atomic Lua**
- A poll loop calls a **Lua script** that does `ZRANGEBYSCORE (due) → XADD to jobs:<queue> → ZREM`
  for each due job, all in one atomic Redis operation. Atomicity matters: a crash mid-move can't
  half-move a job (moved to stream but not removed from the set = duplicate; removed but not moved
  = lost). One script = all-or-nothing.
- Because the move is atomic, running the scheduler inside every worker is safe — each due job is
  claimed by exactly one scheduler. (In a big deployment you'd run a dedicated scheduler or elect a
  leader to avoid redundant polling.)
- `redis.NewScript(...)` in go-redis uses `EVALSHA` with an `EVAL` fallback — the script is cached
  server-side, so we're not shipping the Lua text every call.

**Go**
- `math/rand/v2` needs no seeding; `rand.Int64N(n)` returns `[0, n)`.
- `baseBackoff << (n-1)` shifts a `time.Duration` (an int64) — clean way to do `* 2^(n-1)`; guard
  against overflow (`exp <= 0`) and clamp to a cap.

## Phase 5 — delayed jobs + public client (2026-09-07)

- **Delayed jobs reuse the retry machinery.** `EnqueueIn(delay, ...)` just `Schedule`s the job at
  `now+delay` in the same `jobs:scheduled` ZSET; the scheduler (built in Phase 4) promotes it when
  due. Proven: an `-in 4s` job sat in the ZSET (`XLEN` of the stream = 0) and ran ~4s later.
- **`pkg/jobqueue` is the public API.** Producers import `pkg/*`, never `internal/*` (Go enforces
  this — `internal/` is only importable within the module). The client wraps the broker AND
  publishes the `enqueued` event, so the CLI and the dashboard no longer duplicate that.
- **Functional options** (`WithMaxRetries`, `WithQueue`) — the idiomatic Go way to give a function
  optional, extensible params without a config struct: `func(*job.Job)` closures applied in order.
- `flag.Duration("in", 0, ...)` gives `-in 10s` parsing for free; `flag.Args()` are the positional
  leftovers after flags.

## Phase 6 — reaper (dead-worker recovery) (2026-09-07)

- **`XAUTOCLAIM` is the recovery primitive.** It reassigns pending (delivered-but-unacked) entries
  idle beyond `min-idle-time` to a named consumer and returns them — so a reaper can claim a dead
  worker's stranded jobs and reprocess them. Proven: `kill -9` a worker mid-job → another worker's
  reaper reclaimed the exact same job ID ~4s later, finished it, acked. `XPENDING` went 1 → 0.
- **`minIdle` must exceed the longest job duration.** "Idle" = time since the entry was last
  delivered, and it keeps growing *while a job legitimately runs*. Too small a threshold reclaims
  in-flight work and runs it twice. (Idempotent handlers make that safe, but size it right.)
- **The reaper lives in the worker, feeding the same pool.** It's a second *producer* on the jobs
  channel (alongside the fetcher). Key concurrency detail: both producers must stop before we
  `close(jobs)`, or a producer could send on a closed channel (panic). Solved with a producers
  `sync.WaitGroup` that `Run` waits on before closing.
- Because `XAUTOCLAIM` is atomic, running a reaper in every worker is safe — a stranded entry is
  claimed by exactly one.

## Phase 7 — interactive demo (2026-09-07)

- **Killing a real worker, honestly.** To let the browser "kill a worker" without faking it, one
  `cmd/demo` binary runs the dashboard AND spawns worker **subprocesses** by re-exec'ing itself
  (`os.Executable()` + `exec.Command` with `DJQ_ROLE=worker`). The kill button does
  `cmd.Process.Kill()` (SIGKILL) — a genuine hard crash, so the job is really stranded and the
  reaper really recovers it. Nothing scripted.
- **Decoupling the dashboard from process management.** The dashboard doesn't know how to spawn
  processes — it exposes `SetWorkerControls(add, kill, count)` hooks (func values). `cmd/demo`
  injects the real implementations. So `cmd/dashboard` (no process mgmt) still works; the buttons
  just report "controls off" (the endpoints return 501 and the UI disables them).
- **`exec.Command` hygiene:** always `cmd.Wait()` in a goroutine after `Start()` or you leak
  zombies; kill children on parent shutdown (`defer mgr.killAll()`); children inherit env
  (`os.Environ()`) so `REDIS_ADDR` flows through.
- **Narration is client-side.** The browser already receives every event over SSE, so translating
  them into plain-English sentences ("♻️ worker-2 reclaimed a stranded job…") is just a `switch`
  in JS — no extra server work. Same events, two renderings (the board + the feed).
- **New event `reclaimed`** is emitted by the reaper purely so the UI can narrate recovery; the job
  then flows through the normal started→done events, so its card visibly hops to the new worker.

## Phase 8 — observability (Prometheus + Grafana) (2026-09-07)

- **The event bus is a free metrics aggregation point.** The dashboard already receives every
  transition, so counters/histograms (`jobs_processed_total{type,status}`, `job_duration_seconds`,
  retries, reclaims) come straight from `metrics.Record(event)` in the pump loop — no per-worker
  scrape ports. Gauges (`queue_depth`, `jobs_in_flight`, `scheduled_depth`, `dlq_size`) come from a
  2s Redis poll. Tradeoff: metrics need the dashboard running, and a dashboard restart resets
  counters (Prometheus tolerates counter resets). A huge fleet would instrument each worker and
  scrape all; here one seam is cleaner.
- **`promauto` auto-registers** collectors to the default registry, and `promhttp.Handler()` serves
  them — so exposing `/metrics` is two lines. `CounterVec`/`HistogramVec` take label names up front;
  `.WithLabelValues("send_email","succeeded").Inc()` at call sites.
- **Counters vs gauges vs histograms:** counter = monotonic total (rate() it in PromQL for /sec);
  gauge = point-in-time value that goes up and down (queue depth); histogram = bucketed
  observations, so `histogram_quantile(0.95, rate(..._bucket[5m]))` gives p95 latency.
- **Scraping a host process from a container:** Prometheus in Docker reaches the host dashboard via
  `host.docker.internal:8080` (added `extra_hosts: host-gateway` for Linux parity).
- **Grafana provisioning:** a datasource YAML (pinned `uid: prometheus`) + a dashboards provider
  YAML pointing at a folder of dashboard JSON → the dashboard appears automatically, no clicking.
  Anonymous admin (`GF_AUTH_*`) makes it a zero-friction demo.
- Verified: Prometheus target `up`, `sum(jobs_processed_total)` queryable, Grafana healthy.

## Phase 9 — production infra (2026-09-07)

- **Multi-stage + distroless.** Build in `golang:1.26`, copy the static binaries into
  `gcr.io/distroless/static` (no shell, no libc, non-root). `CGO_ENABLED=0` + `-ldflags="-s -w"`
  gives a self-contained 45MB image. Cache `go mod download` in its own layer (copy go.mod/go.sum
  before the source) so code changes don't re-download deps.
- **THE gotcha: `command` means different things in Compose vs K8s.**
  - docker-compose `command:` → sets **CMD** (args *appended* to ENTRYPOINT). So `command: [worker]`
    on an image whose ENTRYPOINT is `dashboard` runs `dashboard worker` → still the dashboard! Fix:
    use compose `entrypoint:` to replace the binary.
  - Kubernetes `command:` → **overrides ENTRYPOINT** (and `args:` overrides CMD). So `command:
    [worker]` in k8s correctly runs the worker. Same word, opposite behavior — caught it live when
    the "worker" containers all logged "dashboard listening".
- **Unique consumer names when scaling.** Every worker replica must have a distinct consumer name
  in the group (else two processes share one PEL). Defaulting `WORKER_NAME` to the hostname — the
  container/pod name — makes each replica unique for free.
- **`MAXLEN ~` on XADD** caps the stream so acked-but-retained entries don't grow forever; `~`
  (approx) lets Redis trim whole macro-nodes cheaply. Cap is far above realistic in-flight, so
  pending entries aren't trimmed.
- **HPA needs resource requests + metrics-server.** The autoscaler reads CPU as a % of the pod's
  `requests.cpu`, so requests are mandatory for `averageUtilization` to mean anything.
- **Graceful shutdown ties to k8s.** `terminationGracePeriodSeconds` should be ≥ the worker's
  in-flight drain window, so k8s doesn't SIGKILL a pod mid-drain on rollout.
- **Load test result:** ~15,500 enqueues/sec from 8 concurrent producers against one Redis
  (`go run ./cmd/loadtest -n 20000 -p 8`).
- **DLQ re-drive:** `XRANGE` the dead stream, reset `Attempt`, `Enqueue`, `XDEL` — operators
  recover dead jobs after fixing the cause; keeping the same job ID means the dashboard card revives.
