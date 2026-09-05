# Reading Backlog — Distributed Job Queue

> Purpose: what to read, in what order, so I *understand* what I'm building — not just watch it get
> written. Check items off as I go. Ordered by when I need them (just-in-time), not all up front.

Legend: ⏱ time · 🎯 what to focus on · ✅ what I should be able to say afterwards.

---

## Tier 0 — Before Phase 1 (must-do, ~1 hour total)

- [ ] **My own design + the transport ADR** ⏱ 15 min
  - `docs/design.md` (focus §4–§8) and `docs/adr/0002-redis-streams-as-transport.md`
  - 🎯 The job lifecycle (§6) and delivery semantics (§8). I wrote the *what*; the reading below
    makes the *how* concrete.

- [ ] **Redis Streams + consumer groups** — the concept that unlocks everything ⏱ 30–40 min
  - [Redis Streams — official guide](https://redis.io/docs/latest/develop/data-types/streams/)
    — read through, but slow down hard on the **Consumer Groups** section.
  - Command refs to skim so the verbs stick:
    [XREADGROUP](https://redis.io/docs/latest/commands/xreadgroup/) ·
    [XACK](https://redis.io/docs/latest/commands/xack/) ·
    [XAUTOCLAIM](https://redis.io/docs/latest/commands/xautoclaim/)
  - 🎯 The Pending Entries List (PEL): how `XREADGROUP ... >` delivers *and* parks an entry until
    `XACK`, and how `XAUTOCLAIM` recovers a dead consumer's entries.
  - ✅ I can say: "`XADD` appends a job; `XREADGROUP` with `>` delivers new entries and parks them in
    the PEL; the entry stays until I `XACK` (crash-before-ack = not lost); `XAUTOCLAIM` reassigns a
    dead consumer's idle entries → that's the reaper." That's the entire spine of the project.

---

## Tier 1 — Alongside Phase 1 (Go basics, ~1 hour, don't front-load)

Phase 1 Go is beginner-level; each idiom gets explained inline as it's written. Skim to not feel lost.

- [ ] **A Tour of Go** — up through *"Methods and interfaces"* ⏱ ~1 hr (spread out)
  - [go.dev/tour](https://go.dev/tour/)
  - 🎯 packages/imports, functions & multiple return values, structs, slices/maps, and the
    `if err != nil` error style. **Skip generics and concurrency for now** (see Tier 3).

---

## Tier 2 — Reference (open only when we touch that code)

- [ ] [go-redis Streams guide](https://redis.io/docs/latest/develop/use-cases/streaming/go/)
  — the exact `XAdd` / `XReadGroup` / `XGroupCreateMkStream` calls we'll write.
- [ ] [`context` package](https://pkg.go.dev/context) — one paragraph; same concept as Python, Go
  just threads it explicitly through I/O calls.

---

## Tier 3 — Later, when the phase needs it (queued, not now)

- [ ] **Go concurrency** — read carefully right before **Phase 3** (worker pool). This is the Go
  growth area worth the time.
  - [Tour of Go: Concurrency](https://go.dev/tour/concurrency/1) (goroutines, channels, `select`)
  - [Effective Go — errors & concurrency](https://go.dev/doc/effective_go)
  - `context` cancellation for graceful shutdown.
- [ ] **Exponential backoff + jitter** — before **Phase 4** (retries).
  - AWS "Exponential Backoff And Jitter" article (the canonical explainer of *why* jitter).
- [ ] **Redis Lua scripting** — before **Phase 5** (scheduler's atomic ZSET→Stream move).
  - [EVAL / scripting docs](https://redis.io/docs/latest/develop/interact/programmability/eval-intro/)
- [ ] **Server-Sent Events (SSE)** — before the **dashboard** (Phase 2).
  - MDN "Using server-sent events" + Go `http.Flusher` pattern.
- [ ] **Redis Pub/Sub** — before the **event bus** (Phase 2).
  - [Redis Pub/Sub docs](https://redis.io/docs/latest/develop/interact/pubsub/)
- [ ] **Prometheus Go client** — before **Phase 8** (observability).
  - [prometheus/client_golang](https://prometheus.io/docs/guides/go-application/)
- [ ] **Multi-stage Docker builds for Go & `distroless`** — before **Phase 9** (infra).
- [ ] **`go:embed`** — before embedding the dashboard UI into the binary.

---

## Done log (move items here as I finish, with a one-line takeaway)

- _(nothing yet)_
