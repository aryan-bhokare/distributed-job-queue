# PROGRESS — Distributed Job Queue

> Single source of truth for "where we are." Update at the end of every work chunk so any fresh
> session can resume from here.

**Status:** ✅ Phase 1 (vertical slice) complete and proven end-to-end. Next: Phase 2 (event bus +
live dashboard).

## Done
- Public repo: https://github.com/aryan-bhokare/distributed-job-queue
- `docs/design.md` (incl. §15 live dashboard), `docs/adr/0002` (Redis Streams), `docs/adr/0003`
  (dashboard: Pub/Sub + SSE + embedded UI), `docs/reading-backlog.md`.
- **Phase 1 code:** `internal/job` (Job envelope + ULID), `internal/broker` (XADD/XGROUP/
  XREADGROUP/XACK), `internal/worker` (consume→handle→ack loop + handler registry),
  `cmd/worker`, `cmd/enqueue`, `docker-compose.yml` (Redis; port via `REDIS_PORT`).
- Verified: enqueue → consume → handle → ack works; `XPENDING = 0` after processing (PEL empties);
  graceful stop on SIGTERM. `go build`/`go vet` clean. Deps: go-redis/v9, oklog/ulid/v2.

## In flight
- Nothing — Phase 1 committed.

## Next steps
- **Phase 2:** event bus (Redis Pub/Sub `job.events`) emitted from the worker on each transition;
  dashboard server (`cmd/dashboard`, `internal/dashboard`, `internal/events`) streaming over SSE to
  a minimal embedded web UI (`web/`) that lists jobs + live state. Read Tier-3 SSE + Pub/Sub items
  in `docs/reading-backlog.md` first.

## How to run (current state)
- Aryan's machine: other projects hold host 6379, so we run our Redis on 6380:
  `REDIS_PORT=6380 docker compose up -d` then `REDIS_ADDR=localhost:6380 go run ./cmd/worker` and
  `REDIS_ADDR=localhost:6380 go run ./cmd/enqueue send_email '{"to":"x","template":"welcome"}'`.
- Clean clone: plain `docker compose up -d` (6379) + `go run ./cmd/worker` + `go run ./cmd/enqueue …`.
- A demo Redis container is currently running on :6380 (`distributed-job-queue-redis-1`).

## Notes / gotchas learned
- Redis Streams **retain** entries after `XACK` (ack clears the PEL, not the log). `XLEN` stays >0.
  Add `MAXLEN`/`XTRIM` capping in a later phase so the stream doesn't grow unbounded.
- Phase 1 acks on handler failure (to avoid infinite redelivery) — Phase 4 replaces that with
  retry-with-backoff → DLQ.

## Open questions
- Demo job types: keep `send_email` (instant) + `generate_pdf` (slow); maybe add a `call_llm` demo
  once the dashboard exists.
