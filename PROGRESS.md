# PROGRESS — Distributed Job Queue

> Single source of truth for "where we are." Update at the end of every work chunk so any fresh
> session can resume from here.

**Status:** ✅ Phase 2 (event bus + live dashboard) complete and proven. Next: Phase 3 (worker
pool + graceful shutdown).

## Done
- Public repo: https://github.com/aryan-bhokare/distributed-job-queue
- Design + ADRs (0002 streams, 0003 dashboard) + reading-backlog.
- **Phase 1:** Job envelope+ULID, broker (XADD/XGROUP/XREADGROUP/XACK), worker consume→handle→ack,
  cmd/worker, cmd/enqueue, docker-compose. Verified end-to-end.
- **Phase 2:** event bus (`internal/events`, Redis Pub/Sub `job.events`), worker emits transitions
  (enqueued/started/succeeded/failed/no_handler), dashboard server (`internal/dashboard`) with SSE
  hub + in-memory job view + snapshot-on-connect + `POST /api/enqueue` + `/healthz` `/readyz`,
  embedded UI (`web/` via go:embed: index.html/styles.css/app.js — lanes + counters + animations),
  `cmd/dashboard`. Verified: SSE stream shows snapshot + live enqueued→started→succeeded and
  no_handler; `go build`/`go vet` clean.

## In flight
- Nothing — Phase 2 committed.

## Next steps (Phase 3)
- Worker pool: run N handler goroutines per worker (bounded concurrency).
- Graceful shutdown: on SIGTERM stop reading, let in-flight jobs finish + ack, then exit
  (fixes the "ack failed: context canceled" we saw when killing mid-job). Read Tier-3 Go
  concurrency items in reading-backlog first.

## How to run (Aryan's machine — 6379 taken by other projects, so use 6380)
```
REDIS_PORT=6380 docker compose up -d
REDIS_ADDR=localhost:6380 go run ./cmd/dashboard      # open http://localhost:8080
REDIS_ADDR=localhost:6380 go run ./cmd/worker
# enqueue from the dashboard button, or:
REDIS_ADDR=localhost:6380 go run ./cmd/enqueue send_email '{"to":"x","template":"welcome"}'
```
Stop Redis: `REDIS_PORT=6380 docker compose down`. Clean clone uses plain 6379.

## Notes / gotchas
- macOS has no `timeout` cmd — use `curl --max-time N` (learned during SSE testing).
- Streams retain entries after XACK (need MAXLEN capping later).
- Pub/Sub has no replay → dashboard sends a state snapshot on each new SSE connection.
- Killing a worker mid-job leaves an entry in the PEL (unacked) = at-least-once working; the reaper
  (Phase 6) reclaims it. Graceful shutdown (Phase 3) avoids creating those on normal deploys.
