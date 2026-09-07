// Package events is the dashboard's event bus. Components publish job-transition
// events to a Redis Pub/Sub channel; the dashboard subscribes and fans them out
// to browsers. Pub/Sub is fire-and-forget on purpose — observers never block or
// affect the job hot path. If the dashboard is down, jobs are unaffected.
package events

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// Channel is the single Pub/Sub channel all transitions are published to.
const Channel = "job.events"

// Kinds of transition the dashboard cares about.
const (
	Enqueued  = "enqueued"
	Started   = "started"
	Succeeded = "succeeded"
	Retrying  = "retrying"   // failed but will be retried after a backoff delay
	Dead      = "dead"       // exhausted retries → dead-letter queue
	Reclaimed = "reclaimed"  // recovered from a crashed worker's PEL by the reaper
	NoHandler = "no_handler" // no handler registered for this type
)

// Event is one job transition. Small and self-describing so the browser can
// render it without extra lookups.
type Event struct {
	Kind       string    `json:"kind"`
	JobID      string    `json:"job_id"`
	JobType    string    `json:"job_type"`
	Queue      string    `json:"queue"`
	Worker     string    `json:"worker,omitempty"`
	Attempt    int       `json:"attempt"`
	Error      string    `json:"error,omitempty"`
	DurationMs int64     `json:"duration_ms,omitempty"`
	RetryInMs  int64     `json:"retry_in_ms,omitempty"` // backoff before the next retry
	At         time.Time `json:"at"`
}

// Publisher publishes events to the bus.
type Publisher struct{ rdb *redis.Client }

func NewPublisher(rdb *redis.Client) *Publisher { return &Publisher{rdb: rdb} }

// Publish fire-and-forgets an event. Errors are intentionally ignored: a
// publish failure (e.g. dashboard/Redis hiccup) must never fail a job. A nil
// Publisher is a safe no-op, so workers can run without a dashboard.
func (p *Publisher) Publish(ctx context.Context, e Event) {
	if p == nil || p.rdb == nil {
		return
	}
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	data, err := json.Marshal(e)
	if err != nil {
		return
	}
	_ = p.rdb.Publish(ctx, Channel, data).Err()
}

// Subscribe returns a channel of events read from the bus until ctx is cancelled.
// The goroutine + channel is Go's idiomatic way to turn a callback-ish source
// into something you can `range` over.
func Subscribe(ctx context.Context, rdb *redis.Client) <-chan Event {
	out := make(chan Event, 128)
	sub := rdb.Subscribe(ctx, Channel)
	go func() {
		defer close(out)
		defer sub.Close()
		msgs := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgs:
				if !ok {
					return
				}
				var e Event
				if json.Unmarshal([]byte(msg.Payload), &e) == nil {
					select {
					case out <- e:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()
	return out
}
