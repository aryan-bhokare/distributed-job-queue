# PROGRESS — Distributed Job Queue

> Single source of truth for "where we are." Update at the end of every work chunk so any fresh
> session can resume from here.

**Status:** ✅ ALL 9 phases complete and proven. The system is feature-complete, reliable,
observable, and deployable. Remaining work is optional polish (see Ideas).

## Done (all verified end-to-end)
- Public repo: https://github.com/aryan-bhokare/distributed-job-queue
- Design + 3 ADRs + reading-backlog + full LEARNINGS log.
- **P1** vertical slice · **P2** event bus + live dashboard (SSE) · **P3** worker pool + graceful
  shutdown (two-context drain) · **P4** retries + backoff + jitter + DLQ + scheduler (atomic Lua) ·
  **P5** delayed jobs (`pkg/jobqueue`) · **P6** reaper (`XAUTOCLAIM`) · **P7** interactive demo
  (`cmd/demo`, kill-worker button, narration feed) · **P8** observability (Prometheus + Grafana) ·
  **P9** infra: distroless image (45MB), containerized compose (`--profile full`), K8s manifests
  (Deployments/Service/HPA/probes), GitHub Actions CI, `cmd/loadtest` (~15.5k enqueues/sec), MAXLEN
  capping, DLQ re-drive.

## In flight
- Nothing — all phases committed and pushed.

## How to run
```
REDIS_PORT=6380 docker compose up -d
REDIS_ADDR=localhost:6380 go run ./cmd/demo          # http://localhost:8080 (interactive)
# observability:  docker compose --profile obs up -d   → Grafana http://localhost:3000
# full container stack:  docker compose --profile full up --build --scale worker=4
# k8s:  kubectl apply -f deploy/k8s/
# load test:  REDIS_ADDR=localhost:6380 go run ./cmd/loadtest -n 20000 -p 8
```

## Ideas / optional polish (not required)
- kind/minikube deploy walkthrough (validate the k8s manifests on a real cluster).
- Priority queues (one stream per priority; weighted worker poll).
- Unique/dedup jobs via an idempotency-key SET NX guard (the field already exists on the envelope).
- Push the image to GHCR from CI; add a release workflow.
- A short screen-recording / GIF of the dashboard for the README.

## Notes / gotchas (the big ones)
- compose `command` = append-to-ENTRYPOINT; k8s `command` = override-ENTRYPOINT (opposite!).
- Each worker replica needs a unique consumer name → default WORKER_NAME to hostname.
- Reaper `minIdle` must exceed the longest job; two-context drain avoids acking-canceled on stop.
