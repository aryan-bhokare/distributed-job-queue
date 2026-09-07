// Command loadtest floods the queue with jobs from several concurrent producers
// and reports enqueue throughput. Run a worker pool alongside to watch processing
// throughput/latency on the dashboard or in Grafana.
//
//	go run ./cmd/loadtest -n 20000 -p 8 -type send_email
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
)

func main() {
	n := flag.Int("n", 10000, "total jobs to enqueue")
	p := flag.Int("p", 8, "concurrent producers")
	typ := flag.String("type", "send_email", "job type")
	flag.Parse()

	rdb := redis.NewClient(&redis.Options{
		Addr:     envOr("REDIS_ADDR", "localhost:6379"),
		PoolSize: *p * 2,
	})
	defer rdb.Close()
	b := broker.New(rdb)
	ctx := context.Background()

	perProducer := *n / *p
	total := perProducer * *p
	var done int64

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *p; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < perProducer; k++ {
				j, err := job.New(*typ, map[string]any{"i": k})
				if err != nil {
					continue
				}
				if err := b.Enqueue(ctx, j); err == nil {
					atomic.AddInt64(&done, 1)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("enqueued %d/%d %q jobs · %d producers · %s · %.0f jobs/sec\n",
		atomic.LoadInt64(&done), total, *typ, *p, elapsed.Round(time.Millisecond),
		float64(done)/elapsed.Seconds())
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
