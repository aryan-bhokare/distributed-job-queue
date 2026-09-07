# Distributed Job Queue

A Sidekiq/Celery-inspired distributed background job queue in **Go**, on **Redis Streams** —
built for reliability: at-least-once delivery, retries with backoff, dead-letter queues,
dead-worker recovery, delayed jobs, and full observability. Ships with a **live dashboard** that
visualises jobs flowing through the system in real time (you can even kill a worker and watch it
recover).

> Portfolio project focused on depth. The **[system design](docs/design.md)** and the
> **[architecture decisions](docs/adr/)** are documented as I build. Reading path for the concepts:
> **[docs/reading-backlog.md](docs/reading-backlog.md)**.

## Why Redis Streams?

Consumer groups give **at-least-once delivery** (a delivered job stays in a Pending Entries List
until the worker `XACK`s it — a crash before ack doesn't lose it) and **dead-worker recovery**
(`XAUTOCLAIM` re-assigns jobs a crashed worker was holding), on a single dependency. Full rationale
vs. Redis Lists / Kafka / RabbitMQ / Postgres in **[ADR-0002](docs/adr/0002-redis-streams-as-transport.md)**.

## Status

Building in phases (see [design §13](docs/design.md#13-phased-build-plan-each-phase-demoable--committed)):

- [x] **Phase 1 — Vertical slice**: enqueue → worker consumes via a consumer group → runs a handler → acks. Proven end-to-end.
- [x] **Phase 2 — Event bus + live dashboard**: every transition is published to a Redis Pub/Sub bus; a dashboard streams it to the browser over SSE with a live board + small animations. Enqueue from the UI and watch jobs flow.
- [x] **Phase 3 — Worker pool + graceful shutdown**: each worker runs a bounded goroutine pool (`WORKER_CONCURRENCY`); on SIGTERM it stops fetching, lets in-flight jobs finish and ack within a grace window, then exits. Multiple worker processes load-balance via the consumer group.
- [x] **Phase 4 — Retries + backoff + DLQ**: a failed handler is retried with exponential backoff + jitter (job re-scheduled in a sorted set; a scheduler atomically promotes due jobs via a Lua script); after `max_retries` it's dead-lettered to `jobs:dead`. The dashboard shows *Retrying* and *Dead* lanes.
- [x] **Phase 5 — Delayed jobs**: a public client (`pkg/jobqueue`) with `Enqueue` / `EnqueueIn(delay, …)`; delayed jobs wait in the scheduled set and the scheduler promotes them when due. Dashboard "Delay 6s" button + `-in` CLI flag.
- [x] **Phase 6 — Reaper (dead-worker recovery)**: each worker runs a reaper that `XAUTOCLAIM`s entries idle in the PEL past a threshold and reprocesses them — so a job survives a worker being `kill -9`'d mid-execution.
- [ ] Phase 7 — Interactive demo / happy-path tour (guided controls + narration, incl. a "kill worker" button)
- [ ] Phase 8 — Observability (Prometheus + Grafana)
- [ ] Phase 9 — Production infra (Docker, K8s, CI/CD, load test)

## Quickstart

Requires Go 1.26+ and Docker.

```bash
# 1. Start Redis (uses host port 6379; set REDIS_PORT to avoid a clash)
docker compose up -d
#    ...or on a machine where 6379 is taken:
#    REDIS_PORT=6380 docker compose up -d   (then prefix commands with REDIS_ADDR=localhost:6380)

# 2. Run a worker (consumes + executes jobs)
go run ./cmd/worker

# 3. Run the live dashboard, then open http://localhost:8080
go run ./cmd/dashboard

# 4. Enqueue jobs — from the dashboard's "+ Enqueue job" button, or the CLI:
go run ./cmd/enqueue send_email  '{"to":"you@example.com","template":"welcome"}'
go run ./cmd/enqueue generate_pdf '{"doc":"invoice-42"}'
```

Watch the dashboard: each job flows **Queued → Running → Done** in real time (SSE), with small
animations. `generate_pdf` deliberately takes ~1.5s so you can see the async nature. The worker also
logs each transition as structured JSON.

**Scale it** — each worker runs a bounded goroutine pool, and multiple worker processes share the
queue via the consumer group:

```bash
WORKER_CONCURRENCY=10 go run ./cmd/worker                      # 10 jobs in parallel
WORKER_NAME=worker-A go run ./cmd/worker &                     # run several processes;
WORKER_NAME=worker-B go run ./cmd/worker &                     # the group load-balances jobs
```

On `Ctrl-C`/SIGTERM a worker stops fetching, lets in-flight jobs finish and ack (within a grace
window), then exits — no dropped or double-acked work.

## Layout

```
cmd/worker      # runs a worker
cmd/enqueue     # CLI to enqueue a job
cmd/dashboard   # runs the live dashboard (SSE + control API + embedded UI)
internal/job    # Job envelope + ULID
internal/broker # Redis Streams ops (XADD / XREADGROUP / XACK) + scheduled set + DLQ + Lua move
internal/worker # consume → handle → ack loop, handler registry, retry/backoff/DLQ
internal/scheduler # promotes due delayed/retry jobs into their streams (atomic Lua)
internal/events # event bus: publish/subscribe job transitions (Redis Pub/Sub)
internal/dashboard # SSE hub + control handlers
pkg/jobqueue    # public client: Enqueue / EnqueueIn (what producers import)
web/            # dependency-free dashboard UI, embedded via go:embed
docs/           # design.md, ADRs, reading-backlog.md
# (dead-worker recovery — the "reaper" — lives inside internal/worker so it can
#  reuse the pool + handler registry to reprocess reclaimed jobs)
```

## Tech

Go · Redis Streams (consumer groups) · Docker · (coming: Prometheus, Grafana, Kubernetes)
