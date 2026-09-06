# PROGRESS — Distributed Job Queue

> Single source of truth for "where we are." Update at the end of every work chunk so any fresh
> session can resume from here.

**Status:** ✅ Phase 3 (worker pool + graceful shutdown) complete and proven. Next: Phase 4
(retries + backoff + DLQ).

## Done
- Public repo: https://github.com/aryan-bhokare/distributed-job-queue
- Design + ADRs (0002 streams, 0003 dashboard) + reading-backlog.
- **Phase 1:** Job envelope+ULID, broker (XADD/XGROUP/XREADGROUP/XACK), worker consume→handle→ack.
- **Phase 2:** event bus (`internal/events`, Pub/Sub), worker emits transitions, dashboard
  (`internal/dashboard`, SSE hub + snapshot + `/api/enqueue`), embedded UI (`web/`), `cmd/dashboard`.
- **Phase 3:** worker pool (N goroutines fed by an unbuffered channel; `WORKER_CONCURRENCY`),
  graceful shutdown via the two-context pattern (root ctx stops the fetcher; a separate procCtx lets
  in-flight jobs finish+ack within a grace window), `WORKER_NAME` for multiple processes.
  Verified: pool of 3 ran 3 PDFs at once; SIGTERM mid-job finished+acked (XPENDING=0, no ack-fail);
  two workers split 20 jobs 10/10. `go build`/`go vet` clean.

## In flight
- Nothing — Phase 3 committed.

## Next steps (Phase 4 — retries + backoff + DLQ)
- On handler error: if `attempt < max_retries`, re-enqueue into `jobs:scheduled` (ZSET) at
  `now + base*2^attempt + jitter` (needs the scheduler from Phase 5 to move due jobs back — or do a
  simple immediate re-XADD with a delay field for now and formalize in Phase 5). At `attempt ==
  max_retries`, push to `jobs:dead` (DLQ stream) with the error.
- Emit `retrying` / `dead` events so the dashboard shows those lanes.
- Replace the Phase-1 shortcut in `worker.process` (currently acks on failure) with the real path.
- Read Tier-3 "Exponential backoff + jitter" item in reading-backlog first.

## How to run (Aryan's machine — 6379 taken, use 6380)
```
REDIS_PORT=6380 docker compose up -d
REDIS_ADDR=localhost:6380 go run ./cmd/dashboard      # http://localhost:8080
REDIS_ADDR=localhost:6380 WORKER_CONCURRENCY=5 go run ./cmd/worker
REDIS_ADDR=localhost:6380 go run ./cmd/enqueue send_email '{"to":"x","template":"welcome"}'
```
Stop Redis: `REDIS_PORT=6380 docker compose down`.

## Notes / gotchas
- Two-context pattern is what makes shutdown graceful (see LEARNINGS Phase 3).
- go-redis `XReadGroup` Block doesn't abort instantly on ctx-cancel → shutdown can lag ~2s. Fine.
- Streams still retain entries after XACK (MAXLEN capping still TODO, later phase).
- Reaper (Phase 6) will reclaim jobs left in the PEL when grace elapses or a worker hard-crashes.
