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

// Ping verifies Redis is reachable (used by health checks and startup).
func (b *Broker) Ping(ctx context.Context) error { return b.rdb.Ping(ctx).Err() }
