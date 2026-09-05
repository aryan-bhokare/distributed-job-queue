// Package job defines the unit of work that flows through the queue: the Job
// envelope, its JSON (de)serialization, and ID generation. The queue itself
// never looks inside Payload — only the handler registered for a given Type does.
package job

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
)

// DefaultQueue is the queue a job lands on when none is specified.
const DefaultQueue = "default"

// Job is the envelope that travels through the queue as one JSON blob.
//
// Struct tags (the `json:"..."` bits) tell Go's encoding/json how to name each
// field on the wire — this is Go's equivalent of Pydantic field aliases.
type Job struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"` // opaque to the queue; handler decodes it
	Queue          string          `json:"queue"`
	Attempt        int             `json:"attempt"`
	MaxRetries     int             `json:"max_retries"`
	EnqueuedAt     time.Time       `json:"enqueued_at"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// New builds a Job with a fresh ULID and sensible defaults. `payload any` means
// "accept any value" (Go's `any` == `interface{}`); we marshal it to JSON here so
// callers can pass a struct or a map without thinking about serialization.
func New(typ string, payload any) (Job, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		// %w wraps the underlying error so callers can still inspect it — Go's
		// idiomatic way to add context without hiding the cause.
		return Job{}, fmt.Errorf("marshal payload: %w", err)
	}
	return Job{
		ID: ulid.Make().String(), // ULID: time-sortable + collision-resistant, no coordination
		Type:       typ,
		Payload:    raw,
		Queue:      DefaultQueue,
		Attempt:    0,
		MaxRetries: 5,
		EnqueuedAt: time.Now().UTC(),
	}, nil
}

// Marshal is the wire format stored inside a stream entry.
func (j Job) Marshal() ([]byte, error) { return json.Marshal(j) }

// Unmarshal parses a Job back out of a stream entry.
func Unmarshal(b []byte) (Job, error) {
	var j Job
	err := json.Unmarshal(b, &j)
	return j, err
}
