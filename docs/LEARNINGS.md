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
