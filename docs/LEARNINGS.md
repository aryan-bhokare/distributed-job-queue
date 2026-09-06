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
