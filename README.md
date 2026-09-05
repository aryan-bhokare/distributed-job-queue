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
- [ ] Phase 2 — Event bus + live dashboard (SSE)
- [ ] Phase 3 — Worker pool + graceful shutdown
- [ ] Phase 4 — Retries + backoff + DLQ
- [ ] Phase 5 — Delayed jobs + scheduler
- [ ] Phase 6 — Reaper (dead-worker recovery)
- [ ] Phase 7 — Interactive demo / happy-path tour
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

# 3. In another terminal, enqueue jobs
go run ./cmd/enqueue send_email  '{"to":"you@example.com","template":"welcome"}'
go run ./cmd/enqueue generate_pdf '{"doc":"invoice-42"}'
```

You'll see the worker pick up each job, run the matching handler, and log it as done (structured
JSON logs). `generate_pdf` deliberately takes ~1.5s to make the async nature visible.

## Layout

```
cmd/worker      # runs a worker
cmd/enqueue     # CLI to enqueue a job
internal/job    # Job envelope + ULID
internal/broker # Redis Streams ops (XADD / XREADGROUP / XACK)
internal/worker # consume → handle → ack loop + handler registry
docs/           # design.md, ADRs, reading-backlog.md
```

## Tech

Go · Redis Streams (consumer groups) · Docker · (coming: Prometheus, Grafana, Kubernetes)
