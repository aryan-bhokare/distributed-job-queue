# PROGRESS — Distributed Job Queue

> Single source of truth for "where we are." Update at the end of every work chunk so any fresh
> session can resume from here.

**Status:** ✅ Phases 1–7 complete and proven. Next: Phase 8 (observability), then Phase 9
(production infra).

## Done (all verified end-to-end)
- Public repo: https://github.com/aryan-bhokare/distributed-job-queue
- Design + ADRs (0002 streams, 0003 dashboard) + reading-backlog.
- **P1** vertical slice. **P2** event bus (Pub/Sub) + live dashboard (SSE). **P3** worker pool +
  graceful shutdown (two-context drain). **P4** retries + backoff + jitter + DLQ + scheduler (Lua).
  **P5** delayed jobs (`pkg/jobqueue` client, `-in` flag, "Delay" button). **P6** reaper
  (`XAUTOCLAIM`). **P7** interactive demo: `cmd/demo` runs dashboard + real worker subprocesses;
  browser **Kill worker** button (SIGKILL) → reaper recovers; plain-English **narration feed**.
- Latest proof: `cmd/demo` spawned 2 workers; killed worker-1 mid-slow-job via the API; worker-2's
  reaper reclaimed its stranded jobs (`reclaimed` events), `XPENDING` → 0. build/vet clean.

## In flight
- Nothing — Phase 7 committed.

## Next steps
- **Phase 8 (observability):** Prometheus metrics via `prometheus/client_golang` — counters
  (jobs_processed_total{status,type}, jobs_retried_total, dlq_size), a `job_duration_seconds`
  histogram, a `queue_depth` gauge (poll `XLEN`); expose `/metrics` on the worker + dashboard; add a
  Grafana dashboard + a Prometheus/Grafana compose profile. Emit metrics from `worker.process` and
  the enqueue path. (Read the Prometheus-Go item in reading-backlog first.)
- **Phase 9 (infra):** multi-stage distroless Dockerfile; compose stack (app+redis+prometheus+
  grafana); K8s manifests (probes/HPA); GitHub Actions CI (lint→test→build→scan→push); load test;
  `MAXLEN` stream capping; DLQ re-drive tool.

## How to run
```
REDIS_PORT=6380 docker compose up -d
REDIS_ADDR=localhost:6380 go run ./cmd/demo     # ← one command: dashboard + 2 workers, http://localhost:8080
```
Interactive demo: enqueue Slow jobs, then "💀 Kill worker" → watch the reaper reclaim (narrated).
Separate pieces: `cmd/dashboard` + `cmd/worker` + `cmd/enqueue [-in DUR]` still work.

## Notes / gotchas
- `cmd/demo` re-execs itself (`os.Executable`) with `DJQ_ROLE=worker` to spawn workers; kill =
  `Process.Kill()` (SIGKILL). Always `cmd.Wait()` in a goroutine (no zombies); `killAll` on shutdown.
- Reaper `minIdle` (8s in demo) must exceed the longest job (slow = 6s).
- Streams still retain entries after XACK — MAXLEN capping still TODO (Phase 9).
