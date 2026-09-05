// Command enqueue pushes one job onto the queue from the CLI — a quick way to
// exercise the system while building.
//
//	go run ./cmd/enqueue send_email '{"to":"a@b.com","template":"welcome"}'
//	go run ./cmd/enqueue generate_pdf '{"doc":"report"}'
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"

	"github.com/aryan-bhokare/distributed-job-queue/internal/broker"
	"github.com/aryan-bhokare/distributed-job-queue/internal/job"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: enqueue <type> [json-payload]")
		os.Exit(2)
	}
	jobType := os.Args[1]

	var payload any = map[string]any{}
	if len(os.Args) >= 3 {
		if err := json.Unmarshal([]byte(os.Args[2]), &payload); err != nil {
			fmt.Println("invalid JSON payload:", err)
			os.Exit(2)
		}
	}

	rdb := redis.NewClient(&redis.Options{Addr: envOr("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()
	b := broker.New(rdb)

	j, err := job.New(jobType, payload)
	if err != nil {
		fmt.Println("build job:", err)
		os.Exit(1)
	}
	if err := b.Enqueue(context.Background(), j); err != nil {
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
