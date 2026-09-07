// Package jobqueue is the public client for enqueuing work. Producers import this
// (not internal/*): construct a Client, then Enqueue / EnqueueIn. It wraps the
// broker and also publishes the "enqueued" event so every producer shows up on
// the dashboard without duplicating that logic.
package jobqueue

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/events"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
)

// Client enqueues jobs onto the queue.
type Client struct {
	broker *broker.Broker
	pub    *events.Publisher
}

// New builds a Client from a Redis client.
func New(rdb *redis.Client) *Client {
	return &Client{broker: broker.New(rdb), pub: events.NewPublisher(rdb)}
}

// Option customizes a job before it is enqueued.
type Option func(*job.Job)

// WithMaxRetries overrides how many times a job is retried before dead-lettering.
func WithMaxRetries(n int) Option { return func(j *job.Job) { j.MaxRetries = n } }

// WithQueue routes the job to a named queue (default "default").
func WithQueue(name string) Option { return func(j *job.Job) { j.Queue = name } }

// Enqueue adds a job to run as soon as a worker is free.
func (c *Client) Enqueue(ctx context.Context, typ string, payload any, opts ...Option) (job.Job, error) {
	j, err := c.build(typ, payload, opts)
	if err != nil {
		return job.Job{}, err
	}
	if err := c.broker.Enqueue(ctx, j); err != nil {
		return job.Job{}, err
	}
	c.pub.Publish(ctx, events.Event{Kind: events.Enqueued, JobID: j.ID, JobType: j.Type, Queue: j.Queue})
	return j, nil
}

// EnqueueIn schedules a job to become ready after `delay` (delayed jobs). The
// scheduler promotes it into its stream when due.
func (c *Client) EnqueueIn(ctx context.Context, delay time.Duration, typ string, payload any, opts ...Option) (job.Job, error) {
	j, err := c.build(typ, payload, opts)
	if err != nil {
		return job.Job{}, err
	}
	if err := c.broker.Schedule(ctx, j, time.Now().Add(delay)); err != nil {
		return job.Job{}, err
	}
	// RetryInMs doubles as "starts in" so the dashboard can show the countdown.
	c.pub.Publish(ctx, events.Event{Kind: events.Enqueued, JobID: j.ID, JobType: j.Type, Queue: j.Queue, RetryInMs: delay.Milliseconds()})
	return j, nil
}

func (c *Client) build(typ string, payload any, opts []Option) (job.Job, error) {
	j, err := job.New(typ, payload)
	if err != nil {
		return job.Job{}, err
	}
	for _, o := range opts {
		o(&j)
	}
	return j, nil
}
