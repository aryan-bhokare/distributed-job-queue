# PROGRESS — Distributed Job Queue

> Single source of truth for "where we are." Update at the end of every work chunk so any fresh
> session can resume from here.

**Status:** ✅ Phase 4 (retries + backoff + DLQ, incl. the scheduler) complete and proven. Next:
Phase 5 (delayed-jobs `EnqueueIn` API — small, scheduler already exists) then Phase 6 (reaper).

## Done
- Public repo: https://github.com/aryan-bhokare/distributed-job-queue
- Design + ADRs (0002 streams, 0003 dashboard) + reading-backlog.
- **Phase 1:** Job envelope+ULID, broker (XADD/XGROUP/XREADGROUP/XACK), worker consume→handle→ack.
- **Phase 2:** event bus (Pub/Sub), dashboard (SSE hub + snapshot + /api/enqueue), embedded UI.
- **Phase 3:** worker pool (WORKER_CONCURRENCY), graceful shutdown (two-context drain), WORKER_NAME.
- **Phase 4:** broker `Schedule`/`PushDead`/`MoveDue` (atomic Lua) + `jobs:scheduled` ZSET +
  `jobs:dead` DLQ; worker `handleFailure` (retry-with-backoff then DLQ) + `backoff()` (exp + equal
  jitter); `internal/scheduler` promotes due jobs (run inside cmd/worker); events `retrying`/`dead`;
  dashboard Retrying/Dead lanes + counters + Flaky/Always-fail buttons + typed `/api/enqueue?type=`.
  Verified: flaky job retried twice (backoff 0.4s→0.85s) then succeeded; always_fail (max_retries=3)
  dead-lettered after 4 tries (backoff 0.5s→1.2s→2.0s); DLQ=1, scheduled=0, PEL=0. build/vet clean.

## In flight
- Nothing — Phase 4 committed.

## Next steps
- **Phase 5:** add `Broker.EnqueueIn(ctx, delay, job)` (= Schedule at now+delay) + a `pkg/jobqueue`
  public client, and a CLI flag / dashboard control for delayed enqueue. Scheduler already moves them.
- **Phase 6:** reaper — `XAUTOCLAIM` entries idle in the PEL past a threshold (recovers jobs from
  hard-crashed workers / scheduling-write failures). Add a dashboard "kill worker" demo.

## How to run (Aryan's machine — 6379 taken, use 6380)
```
REDIS_PORT=6380 docker compose up -d
REDIS_ADDR=localhost:6380 go run ./cmd/dashboard      # http://localhost:8080
REDIS_ADDR=localhost:6380 WORKER_CONCURRENCY=5 go run ./cmd/worker
# dashboard buttons: "+ Job", "Flaky (retries)", "Always-fail (DLQ)", "Burst ×10"
```
Stop Redis: `REDIS_PORT=6380 docker compose down`.

## Notes / gotchas
- Ack the failing entry only AFTER the retry is scheduled / DLQ'd; on a scheduling failure, leave it
  in the PEL for the reaper.
- Scheduler uses one atomic Lua move → safe to run in every worker.
- Streams still retain entries after XACK (MAXLEN capping still TODO).
- DLQ (`jobs:dead`) is a stream; a re-drive tool (move dead → back to queue) would be a nice extra.
