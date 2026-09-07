// Package broker wraps the Redis Streams operations the queue is built on:
// enqueue (XADD), consumer-group setup (XGROUP CREATE), read (XREADGROUP), and
// acknowledge (XACK). Isolating Redis here keeps those details out of the rest
// of the system and gives us one place to reason about the transport.
package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
)

// ConsumerGroup is the single group all workers share. One group = one shared
// Pending Entries List (PEL) across every worker, which is what gives us both
// load-balancing (Redis hands each entry to exactly one consumer) and
// at-least-once delivery (entries stay in the PEL until XACK'd).
const ConsumerGroup = "workers"

// ScheduledKey is a sorted set of not-yet-ready jobs (delayed jobs + pending
// retries), scored by run-at time in unix milliseconds. The scheduler moves due
// members into their stream.
const ScheduledKey = "jobs:scheduled"

// DeadKey is the dead-letter stream: jobs that exhausted their retries land here
// (with the last error) instead of looping forever or being lost.
const DeadKey = "jobs:dead"

// streamKey maps a queue name to its Redis Stream key: "default" -> "jobs:default".
func streamKey(queue string) string { return "jobs:" + queue }

// Broker is a thin, testable wrapper around a Redis client.
type Broker struct {
	rdb *redis.Client
}

func New(rdb *redis.Client) *Broker { return &Broker{rdb: rdb} }

// Enqueue appends a job to its queue's stream. The "*" entry ID (set by go-redis
// when we omit ID) tells Redis to assign a time-ordered ID. The whole Job rides
// in a single field named "data".
func (b *Broker) Enqueue(ctx context.Context, j job.Job) error {
	data, err := j.Marshal()
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	return b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey(j.Queue),
		Values: map[string]any{"data": data},
		// Cap the stream so acked-but-retained entries don't grow unbounded.
		// Approx (~) lets Redis trim efficiently in whole macro-nodes. The cap is
		// far above any realistic in-flight count, so pending entries aren't trimmed.
		MaxLen: 100_000,
		Approx: true,
	}).Err()
}

// EnsureGroup creates the consumer group if it doesn't exist. MkStream also
// creates the stream when missing, so a worker can start before any job exists.
// The "0" start ID means "this group should see the stream from the beginning"
// (so jobs enqueued before the group existed aren't skipped).
func (b *Broker) EnsureGroup(ctx context.Context, queue string) error {
	err := b.rdb.XGroupCreateMkStream(ctx, streamKey(queue), ConsumerGroup, "0").Err()
	// If the group already exists Redis returns a BUSYGROUP error — that's the
	// happy "already set up" case, so we swallow it.
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create group: %w", err)
	}
	return nil
}

// Delivered pairs a job with the stream entry ID we must XACK once it's done.
type Delivered struct {
	EntryID string
	Job     job.Job
}

// Read blocks up to `block` for new entries for this consumer. The special ">"
// ID means "give me entries this group has never delivered to anyone". Delivered
// entries are recorded in the PEL until Ack'd — that's the at-least-once guarantee.
func (b *Broker) Read(ctx context.Context, queue, consumer string, count int64, block time.Duration) ([]Delivered, error) {
	res, err := b.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: consumer,
		Streams:  []string{streamKey(queue), ">"},
		Count:    count,
		Block:    block,
	}).Result()
	if err != nil {
		// redis.Nil here means "block window elapsed with no new entries" — normal.
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("xreadgroup: %w", err)
	}

	var out []Delivered
	for _, stream := range res {
		for _, msg := range stream.Messages {
			raw, _ := msg.Values["data"].(string)
			j, err := job.Unmarshal([]byte(raw))
			if err != nil {
				// A corrupt entry would otherwise wedge the PEL forever. Ack it and
				// move on. (A later phase routes poison entries to the DLQ instead.)
				_ = b.Ack(ctx, queue, msg.ID)
				continue
			}
			out = append(out, Delivered{EntryID: msg.ID, Job: j})
		}
	}
	return out, nil
}

// Ack removes an entry from the group's PEL after successful processing.
func (b *Broker) Ack(ctx context.Context, queue, entryID string) error {
	return b.rdb.XAck(ctx, streamKey(queue), ConsumerGroup, entryID).Err()
}

// Schedule places a job in the scheduled set to become ready at runAt. Used for
// delayed jobs and for retries with backoff.
func (b *Broker) Schedule(ctx context.Context, j job.Job, runAt time.Time) error {
	data, err := j.Marshal()
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	return b.rdb.ZAdd(ctx, ScheduledKey, redis.Z{
		Score:  float64(runAt.UnixMilli()),
		Member: data,
	}).Err()
}

// PushDead sends a job to the dead-letter stream with the error that killed it.
func (b *Broker) PushDead(ctx context.Context, j job.Job, errMsg string) error {
	data, err := j.Marshal()
	if err != nil {
		return fmt.Errorf("marshal job: %w", err)
	}
	return b.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: DeadKey,
		Values: map[string]any{"data": data, "error": errMsg},
	}).Err()
}

// moveDueScript atomically moves every due job (score <= now) from the scheduled
// set into its own stream. Doing the read-move-delete in one Lua script means a
// crash mid-move can never double-move or lose a job — the whole thing is one
// atomic Redis operation.
var moveDueScript = redis.NewScript(`
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, tonumber(ARGV[2]))
for _, member in ipairs(due) do
  local job = cjson.decode(member)
  redis.call('XADD', 'jobs:' .. job.queue, '*', 'data', member)
  redis.call('ZREM', KEYS[1], member)
end
return #due
`)

// MoveDue promotes all jobs whose run-at has passed into their streams, returning
// how many were moved. Safe to call from multiple schedulers concurrently — the
// Lua script is atomic, so each due job is moved exactly once.
func (b *Broker) MoveDue(ctx context.Context, now time.Time, limit int64) (int, error) {
	n, err := moveDueScript.Run(ctx, b.rdb, []string{ScheduledKey}, now.UnixMilli(), limit).Int()
	if err != nil {
		return 0, fmt.Errorf("move due: %w", err)
	}
	return n, nil
}

// Reap claims stream entries that have been pending (delivered but un-acked) for
// longer than minIdle, reassigning them to `consumer`. This is how we recover
// jobs a crashed worker left stranded in the PEL. It returns the claimed jobs,
// which the caller processes + acks like any normal delivery.
//
// NOTE: `minIdle` must exceed your longest expected job duration. A job still
// legitimately being processed also grows "idle" (time since delivery), so too
// small a threshold would reclaim in-flight work and run it twice. (Idempotent
// handlers make that safe, but avoid it by sizing minIdle correctly.)
func (b *Broker) Reap(ctx context.Context, queue, consumer string, minIdle time.Duration, count int64) ([]Delivered, error) {
	msgs, _, err := b.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   streamKey(queue),
		Group:    ConsumerGroup,
		Consumer: consumer,
		MinIdle:  minIdle,
		Start:    "0-0", // scan the whole PEL from the start
		Count:    count,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("xautoclaim: %w", err)
	}

	var out []Delivered
	for _, msg := range msgs {
		raw, _ := msg.Values["data"].(string)
		j, jerr := job.Unmarshal([]byte(raw))
		if jerr != nil {
			_ = b.Ack(ctx, queue, msg.ID) // corrupt entry: drop it
			continue
		}
		out = append(out, Delivered{EntryID: msg.ID, Job: j})
	}
	return out, nil
}

// QueueLen is the number of entries retained in a queue's stream (ready + already
// processed-but-not-trimmed). ScheduledLen and DeadLen size the scheduled set and
// the DLQ. These back the observability gauges.
func (b *Broker) QueueLen(ctx context.Context, queue string) (int64, error) {
	return b.rdb.XLen(ctx, streamKey(queue)).Result()
}
func (b *Broker) ScheduledLen(ctx context.Context) (int64, error) {
	return b.rdb.ZCard(ctx, ScheduledKey).Result()
}
func (b *Broker) DeadLen(ctx context.Context) (int64, error) {
	return b.rdb.XLen(ctx, DeadKey).Result()
}

// PendingLen is the size of the group's Pending Entries List (in-flight jobs).
func (b *Broker) PendingLen(ctx context.Context, queue string) (int64, error) {
	res, err := b.rdb.XPending(ctx, streamKey(queue), ConsumerGroup).Result()
	if err != nil {
		return 0, err
	}
	return res.Count, nil
}

// Redrive moves up to `limit` jobs out of the dead-letter queue back onto their
// queue with a fresh retry budget (Attempt reset). Operators use this after
// fixing whatever made the jobs fail. Returns how many were re-driven.
func (b *Broker) Redrive(ctx context.Context, limit int64) (int, error) {
	msgs, err := b.rdb.XRangeN(ctx, DeadKey, "-", "+", limit).Result()
	if err != nil {
		return 0, fmt.Errorf("read dlq: %w", err)
	}
	n := 0
	for _, m := range msgs {
		raw, _ := m.Values["data"].(string)
		j, jerr := job.Unmarshal([]byte(raw))
		if jerr != nil {
			_ = b.rdb.XDel(ctx, DeadKey, m.ID).Err() // drop unparseable entry
			continue
		}
		j.Attempt = 0 // give it a fresh set of retries
		if err := b.Enqueue(ctx, j); err != nil {
			return n, err
		}
		if err := b.rdb.XDel(ctx, DeadKey, m.ID).Err(); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// Ping verifies Redis is reachable (used by health checks and startup).
func (b *Broker) Ping(ctx context.Context) error { return b.rdb.Ping(ctx).Err() }
