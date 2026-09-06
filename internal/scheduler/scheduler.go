// Package scheduler promotes due jobs from the scheduled set (delayed jobs and
// pending retries) into their live streams. It's a simple poll loop over an
// atomic Lua move, so running more than one scheduler is safe — each due job is
// moved exactly once.
package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
)

type Scheduler struct {
	broker   *broker.Broker
	interval time.Duration
	batch    int64
	log      *slog.Logger
}

func New(b *broker.Broker) *Scheduler {
	return &Scheduler{
		broker:   b,
		interval: 500 * time.Millisecond, // how often to check for due jobs
		batch:    100,                     // max jobs moved per tick
		log:      slog.Default().With("svc", "scheduler"),
	}
}

// Run polls until ctx is cancelled, moving due jobs each tick.
func (s *Scheduler) Run(ctx context.Context) {
	s.log.Info("scheduler started", "interval", s.interval.String())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopped")
			return
		case <-ticker.C:
			n, err := s.broker.MoveDue(ctx, time.Now(), s.batch)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				s.log.Error("move due failed", "err", err)
				continue
			}
			if n > 0 {
				s.log.Info("promoted due jobs to their streams", "count", n)
			}
		}
	}
}
