# ADR 0002 — Redis Streams as the job transport

**Status:** Accepted · **Date:** 2026-09-05

## Context

The queue needs a transport/broker that supports: fast enqueue, concurrent load-balanced consumption
across many workers, **acknowledgement** (so a job survives a worker crash), and **recovery**
(re-deliver a job a dead worker was holding). We also want low operational overhead — this is a
portfolio project, and the whole system already leans on Redis for state.

## Decision

Use a **Redis Stream** per named queue, consumed via a **consumer group**.

- Enqueue = `XADD jobs:{queue} * data <json>`.
- Consume = `XREADGROUP GROUP workers <consumer> COUNT n BLOCK t STREAMS jobs:{queue} >`.
- Acknowledge = `XACK jobs:{queue} workers <id>` only after the handler succeeds.
- Recover = `XAUTOCLAIM` re-assigns entries idle beyond a threshold from dead consumers.

Delayed jobs use a **Sorted Set** (`jobs:scheduled`, score = run-at); a scheduler moves due jobs
into the stream. The DLQ is another stream (`jobs:dead`).

## Consequences

**Positive**
- Ack + Pending Entries List (PEL) give **at-least-once** delivery natively — the core requirement.
- `XAUTOCLAIM` gives **dead-worker recovery** without us building a lease/heartbeat table.
- Consumer groups **load-balance** entries across workers automatically → horizontal scaling is free.
- One dependency (Redis) for broker + scheduled set + DLQ + dedup + metrics-source. Low ops burden.
- Teaches the exact primitives real systems use (this is close to how Sidekiq Pro / Redis-backed
  queues actually work).

**Negative / limits**
- Redis throughput is the ceiling (~100k ops/s single instance) — mitigated by sharding per queue.
- Durability = Redis persistence (AOF/RDB). A hard Redis crash can lose the last fsync window.
  Acceptable for v1; documented. (Kafka would be more durable but far heavier.)
- The PEL grows if we forget to ack or trim; we must manage `XACK`/`XTRIM` discipline.

## Alternatives considered

| Option | Why not (for v1) |
|---|---|
| **Redis List** (`LPUSH`/`BRPOP` / `BRPOPLPUSH`) | Simplest, but pop is destructive — no ack, so a crash after pop loses the job. `BRPOPLPUSH` into a processing list can approximate reliability but you then hand-build recovery, visibility, and dedup that Streams give natively. Streams are strictly better here. |
| **Kafka** | Excellent durability and throughput, and the "right" answer at huge scale — but heavy to run (ZooKeeper/KRaft, partitions), models a partitioned *log* not a work queue (no per-message ack; consumer-group offsets, not per-job PEL), and per-job retry/delay/DLQ semantics are awkward. Overkill for this project; I can *discuss* it as the 100x evolution. |
| **RabbitMQ** | Purpose-built broker with real per-message ack, DLX, delayed-message plugin — arguably the most "correct" fit. Rejected only to keep one datastore (Redis) and because building the reliability on Redis Streams is more *educational* (I implement the mechanisms rather than getting them free). Worth naming as a production alternative. |
| **Postgres (`SELECT … FOR UPDATE SKIP LOCKED`)** | Great when you already run Postgres and want transactional enqueue with your business data. Lower throughput ceiling, polling-based, and less of a "distributed systems" showcase. A strong real-world choice; not the best teaching vehicle for streams/PEL/claim mechanics. |

**Summary for interviews:** "I chose Redis Streams because consumer groups give me at-least-once
delivery (ack + PEL) and dead-worker recovery (`XAUTOCLAIM`) natively, on one dependency. I'd reach
for RabbitMQ or Kafka if I needed stronger durability or partitioned throughput — and I can explain
exactly when that trade tips."
