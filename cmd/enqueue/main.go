// Command enqueue pushes one job onto the queue from the CLI — a quick way to
// exercise the system while building.
//
//	go run ./cmd/enqueue send_email '{"to":"a@b.com","template":"welcome"}'
//	go run ./cmd/enqueue -in 10s generate_pdf '{"doc":"report"}'   # delayed 10s
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/pkg/jobqueue"
)

func main() {
	in := flag.Duration("in", 0, "delay before the job becomes ready, e.g. 10s (0 = run now)")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("usage: enqueue [-in DURATION] <type> [json-payload]")
		os.Exit(2)
	}
	jobType := args[0]

	var payload any = map[string]any{}
	if len(args) >= 2 {
		if err := json.Unmarshal([]byte(args[1]), &payload); err != nil {
			fmt.Println("invalid JSON payload:", err)
			os.Exit(2)
		}
	}

	rdb := redis.NewClient(&redis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()
	client := jobqueue.New(rdb)
	ctx := context.Background()

	if *in > 0 {
		j, err := client.EnqueueIn(ctx, *in, jobType, payload)
		if err != nil {
			fmt.Println("enqueue:", err)
			os.Exit(1)
		}
		fmt.Printf("scheduled %s job %s to run in %s\n", j.Type, j.ID, in.String())
		return
	}

	j, err := client.Enqueue(ctx, jobType, payload)
	if err != nil {
		fmt.Println("enqueue:", err)
		os.Exit(1)
	}
	fmt.Printf("enqueued %s job %s\n", j.Type, j.ID)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
