// Package queue owns all Redis interactions for the job-submitter service.
// It is the only place in this service that writes to Redis, which makes the
// data schema easy to reason about and test in isolation.
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const (
	// JobQueueKey is the Redis list that Service B (worker) watches with BRPOP.
	// Both services must agree on this key — it is the only coupling between them.
	JobQueueKey = "job_queue"

	// jobHashPrefix is prepended to every job ID to form a Redis Hash key.
	// Example: "job:7f3a1b2c-..." holds {status, type, submitted_at, result}.
	jobHashPrefix = "job:"
)

// ValidJobTypes is the authoritative set of job types this system understands.
// The handler validates against this set; the worker uses the same strings as
// its dispatch keys. Keeping the list here (rather than in the handler) means
// there is one place to extend when a new job type is added.
var ValidJobTypes = map[string]bool{
	"prime":  true,
	"bcrypt": true,
	"sort":   true,
}

// Job is the payload serialised to JSON and pushed onto the Redis list.
// The worker deserialises exactly this struct.
type Job struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	SubmittedAt int64  `json:"submitted_at"` // Unix timestamp (seconds)
}

// JobStatus is what the /status/:id handler reads back from Redis.
type JobStatus struct {
	ID          string `json:"job_id"`
	Type        string `json:"type"`
	Status      string `json:"status"`
	SubmittedAt int64  `json:"submitted_at"`
	// Result and Error are only populated once the worker has finished.
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// PushJob creates a new job, stores its metadata in a Redis Hash, and enqueues
// the JSON payload onto the job_queue list.
//
// Two writes are intentionally separated:
//  1. HSET  — store metadata first so /status/:id always finds the job even if
//             the caller crashes before LPUSH completes (extremely rare, but correct).
//  2. LPUSH — add the job to the queue so a worker can pick it up.
//
// Returns the new job ID on success.
func PushJob(ctx context.Context, rdb *redis.Client, jobType string) (string, error) {
	id := uuid.New().String()
	now := time.Now().Unix()

	job := Job{
		ID:          id,
		Type:        jobType,
		SubmittedAt: now,
	}

	payload, err := json.Marshal(job)
	if err != nil {
		return "", fmt.Errorf("marshal job: %w", err)
	}

	// 1. Persist job metadata so /status/:id works immediately after submit.
	hashKey := jobHashPrefix + id
	if err := rdb.HSet(ctx, hashKey, map[string]any{
		"status":       "pending",
		"type":         jobType,
		"submitted_at": now,
	}).Err(); err != nil {
		return "", fmt.Errorf("hset job metadata: %w", err)
	}

	// 2. Push the full payload onto the queue for the worker to consume.
	if err := rdb.LPush(ctx, JobQueueKey, payload).Err(); err != nil {
		// Best-effort cleanup: mark the job as error so status checks don't
		// leave the caller waiting forever for a job that will never run.
		_ = rdb.HSet(ctx, hashKey, "status", "error", "error", "failed to enqueue")
		return "", fmt.Errorf("lpush job_queue: %w", err)
	}

	return id, nil
}

// GetJobStatus reads the Redis Hash for the given job ID and returns its current
// state. Returns (nil, nil) when the job does not exist so the handler can
// distinguish "not found" from a Redis error.
func GetJobStatus(ctx context.Context, rdb *redis.Client, id string) (*JobStatus, error) {
	hashKey := jobHashPrefix + id

	vals, err := rdb.HGetAll(ctx, hashKey).Result()
	if err != nil {
		return nil, fmt.Errorf("hgetall %s: %w", hashKey, err)
	}
	if len(vals) == 0 {
		// Redis returned an empty map — the key does not exist.
		return nil, nil
	}

	var submittedAt int64
	if v, ok := vals["submitted_at"]; ok {
		fmt.Sscanf(v, "%d", &submittedAt)
	}

	return &JobStatus{
		ID:          id,
		Type:        vals["type"],
		Status:      vals["status"],
		SubmittedAt: submittedAt,
		Result:      vals["result"],
		Error:       vals["error"],
	}, nil
}
