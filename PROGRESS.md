# PROGRESS — Distributed Job Queue

> Single source of truth for "where we are." Update at the end of every work chunk so any fresh
> session can resume from here.

**Status:** Design phase — docs written, awaiting final sign-off to start Phase 1 (vertical slice).

## Done
- Project scaffolded; own git repo.
- **Public GitHub repo:** https://github.com/aryan-bhokare/distributed-job-queue
- `docs/design.md` — full system design (Go + Redis Streams; at-least-once + idempotency;
  retries/backoff/DLQ; delayed jobs; reaper; observability; **live dashboard §15**).
- `docs/adr/0002` — Redis Streams as transport (vs List/Kafka/RabbitMQ/Postgres).
- `docs/adr/0003` — Live dashboard: Redis Pub/Sub event bus + SSE + embedded dependency-free UI.

## In flight
- Awaiting sign-off on the (now dashboard-inclusive) design before coding.

## Next steps (build plan — see design §13)
1. Vertical slice: Job envelope + Enqueue to stream + one worker (consume/run/ack) + docker-compose.
2. Event bus + minimal live dashboard (SSE) — watch everything from here on.
3. Worker pool + graceful shutdown.
4. Retries + backoff + DLQ.
5. Delayed jobs + scheduler (Lua).
6. Reaper (XAUTOCLAIM) + dashboard "kill worker" demo.
7. Interactive happy-path tour.
8. Observability (Prometheus/Grafana).
9. Production infra (Docker/K8s/CI-CD/load test).

## How to run (current state)
- Nothing runnable yet (no Go code). Toolchain confirmed: Go 1.26.4, Docker 29.2.

## Open questions
- Demo job type for the dashboard: generic (email) vs something tied to Aryan's experience
  (PDF/DOCX generation, or an LLM call). Leaning toward a couple of built-in demo handlers.
