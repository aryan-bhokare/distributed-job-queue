# ADR 0003 — Live dashboard: event bus + SSE + embedded dependency-free UI

**Status:** Accepted · **Date:** 2026-09-05

## Context

The queue is invisible — its value (reliability engineering) can't be seen. We want a minimal live
dashboard that visualises every job and its state, animates what's happening, and lets a visitor
drive the system (enqueue, fail, delay, kill a worker) and watch it react. Primary audience:
recruiters/interviewers who won't read code. It must not add load or coupling to the hot path.

Three sub-decisions: (a) how components surface job transitions, (b) how those reach the browser,
(c) how the frontend is built and shipped.

## Decision

**(a) Event bus — Redis Pub/Sub.** Every component (worker, scheduler, reaper) publishes a small
JSON event on each job transition to a `job.events` Pub/Sub channel. The dashboard subscribes.

**(b) Browser transport — Server-Sent Events (SSE).** The dashboard server holds an SSE hub;
browsers connect to `GET /events` and receive the transition stream. Visitor actions are ordinary
`POST`s to control endpoints.

**(c) Frontend — dependency-free, `go:embed`-ed.** One `index.html` + vanilla JS + CSS/SVG, embedded
into the Go binary. No framework, no build step. The system ships as one binary + Redis.

## Consequences

**Positive**
- Observers are fully decoupled: if the dashboard is down or slow, **jobs are unaffected** — Pub/Sub
  is fire-and-forget and the workers never block on a subscriber.
- SSE is the minimum viable real-time transport: one-way, plain HTTP, built-in auto-reconnect, no
  handshake/framing to implement. Matches the ~entirely server→client data flow.
- Single-binary deploy (worker+dashboard+UI) → trivial to run locally and in K8s; great demo story.
- No frontend toolchain → nothing to maintain, fast to load, aligns with the workspace's minimal-UI
  philosophy.

**Negative / limits**
- Pub/Sub has **no history/replay** — a browser that connects late misses prior events. Acceptable:
  the dashboard is a live view, not a system of record. If we want backfill, we can seed initial
  state from a `GET /api/state` snapshot on connect (planned) and/or mirror events to a capped
  Stream later.
- SSE is one-way; visitor actions need separate `POST` endpoints (fine — they're discrete commands).
- SSE over HTTP/1.1 is limited by the ~6-connections-per-domain browser cap; irrelevant for a
  single dashboard tab, noted for completeness.
- Vanilla JS means more manual DOM/animation code than a framework — a deliberate trade for zero
  deps on a small UI.

## Alternatives considered

**(a) Surfacing transitions**
| Option | Why not |
|---|---|
| Dashboard **polls Redis** for job state | Simpler, but laggy and hammers Redis; no clean "event" to animate. |
| Reuse the **DLQ/streams** as the event source | Streams are the *work*, not an observability feed; mixing concerns and adds PEL noise. A dedicated Pub/Sub channel is cleaner. |
| A **capped Redis Stream** for events (instead of Pub/Sub) | Gives replay/history — genuinely nice — but adds trim management and read-cursor logic for v1. We start with Pub/Sub and can add a mirrored event stream if we want backfill. |

**(b) Browser transport**
| Option | Why not |
|---|---|
| **WebSockets** | Bidirectional and powerful, but overkill: our client→server needs are a handful of discrete commands, better as REST `POST`s. WS adds a handshake, framing, and ping/pong keepalive to hand-manage. SSE covers the streaming half with far less code. |
| **Long polling** | Works everywhere but is clunky and chattier than SSE for a continuous stream. |

**(c) Frontend**
| Option | Why not |
|---|---|
| **React/Vue + build step** | Nicer DX at scale, but drags in Node tooling, a bundler, and a `node_modules` for a small dashboard — against the minimal, single-binary goal. |
| **Serve static files from disk** | Fine, but embedding via `go:embed` means one artifact to ship and nothing to path-configure in prod. |

**Interview summary:** "Transitions go onto a Redis Pub/Sub bus so observers are decoupled from the
hot path; the dashboard streams them to the browser over SSE — one-way, plain HTTP, auto-reconnect —
and the UI is dependency-free and embedded in the binary, so the whole thing ships as one Go binary
plus Redis. Visitor actions are REST POSTs, so I never needed WebSockets."
