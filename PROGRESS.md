# PROGRESS — Distributed Job Queue

> Single source of truth for "where we are." Update at the end of every work chunk so any fresh
> session can resume from here.

**Status:** ✅ Phases 1–6 complete and proven. Next: Phase 7 (interactive demo / happy-path tour),
then Phase 8 (observability), Phase 9 (production infra).

## Done (all verified end-to-end)
- Public repo: https://github.com/aryan-bhokare/distributed-job-queue
- Design + ADRs (0002 streams, 0003 dashboard) + reading-backlog.
- **P1** vertical slice (enqueue→consume→handle→ack). **P2** event bus (Pub/Sub) + live dashboard
  (SSE). **P3** worker pool + graceful shutdown (two-context drain). **P4** retries + backoff +
  jitter + DLQ + scheduler (atomic Lua move). **P5** delayed jobs (`pkg/jobqueue` client with
  `Enqueue`/`EnqueueIn`, `-in` CLI flag, dashboard "Delay 6s"). **P6** reaper — `XAUTOCLAIM`
  reclaims PEL entries idle past `REAPER_MIN_IDLE` and reprocesses them.
- Latest proofs: delayed `-in 4s` job waited then ran; `kill -9` a worker mid-job → another
  worker's reaper recovered the same job ID, `XPENDING` 1→0. build/vet clean.

## In flight
- Nothing — Phases 5 & 6 committed.

## Next steps
- **Phase 7 (interactive tour):** guided "happy-path" narration + a real **"kill worker" button**.
  Needs the dashboard to manage worker *processes* (spawn/kill) so a visitor can trigger the reaper
  demo from the browser. Consider: dashboard launches N worker subprocesses it can kill, or an
  in-process demo-worker it can cancel. Emit a `reclaimed` event so the UI narrates recovery.
- **Phase 8 (observability):** Prometheus metrics (jobs_processed_total{status}, job_duration
  histogram, queue_depth gauge, retries, dlq_size) at `/metrics`; Grafana dashboard.
- **Phase 9 (infra):** multi-stage Dockerfile (distroless), full compose stack (app+redis+prometheus
  +grafana), K8s manifests (probes/HPA), GitHub Actions CI/CD, load test. Also: MAXLEN stream capping,
  a DLQ re-drive tool.

## How to run (Aryan's machine — 6379 taken, use 6380)
```
REDIS_PORT=6380 docker compose up -d
REDIS_ADDR=localhost:6380 go run ./cmd/dashboard      # http://localhost:8080
REDIS_ADDR=localhost:6380 WORKER_CONCURRENCY=5 go run ./cmd/worker
REDIS_ADDR=localhost:6380 go run ./cmd/enqueue -in 8s send_email '{"to":"x"}'   # delayed
# dashboard buttons: + Job · Delay 6s · Flaky (retries) · Always-fail (DLQ) · Burst ×10
```
Reaper demo: run two workers (WORKER_NAME=A / =B, REAPER_MIN_IDLE=4s), enqueue a generate_pdf,
`kill -9` the one processing it, watch the other reclaim it.

## Notes / gotchas
- Reaper `minIdle` MUST exceed longest job duration (else it reaps live in-flight jobs).
- Both jobs-channel producers (fetcher + reaper) must stop before `close(jobs)` (producers WaitGroup).
- Streams still retain entries after XACK — MAXLEN capping still TODO (Phase 9).
