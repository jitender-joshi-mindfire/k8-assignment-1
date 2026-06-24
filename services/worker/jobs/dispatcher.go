package jobs

import (
	"encoding/json"
	"fmt"
	"time"
)

// Job is the payload deserialised from the Redis queue.
// It must match exactly the struct that job-submitter serialises in queue/redis.go.
// Both services share the same JSON field names — this is the only coupling
// between Service A and Service B.
type Job struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	SubmittedAt int64  `json:"submitted_at"`
}

// Result carries the outcome of a dispatched job.
type Result struct {
	// Output is the human-readable result string produced by the processor.
	// Stored in Redis so /status/:id can return it to the original submitter.
	Output string

	// Duration is the wall-clock time spent inside the processor function.
	// The consumer records this into the Prometheus histogram.
	Duration time.Duration

	// Err is non-nil when the job processor returned an error.
	// Currently only bcrypt.GenerateFromPassword can fail (invalid cost value).
	// Prime and sort are infallible.
	Err error
}

// Dispatch parses the raw JSON payload from the Redis queue, routes it to the
// correct processor based on Job.Type, and returns the Result.
//
// Design: the switch-based dispatch is intentional over a map[string]func().
// A switch is:
//   - Readable: every case is visible at a glance, no indirection
//   - Safe:     the compiler catches typos in case strings (strings are literals)
//   - Extensible: adding a new job type means adding one case and one import
//
// Adding a new job type requires:
//  1. A new case in this switch
//  2. A new Run*() function in this package
//  3. Adding the type string to queue.ValidJobTypes in job-submitter
func Dispatch(payload []byte) Result {
	var job Job
	if err := json.Unmarshal(payload, &job); err != nil {
		return Result{Err: fmt.Errorf("unmarshal job payload: %w", err)}
	}

	start := time.Now()

	switch job.Type {
	case "prime":
		output := RunPrime()
		return Result{Output: output, Duration: time.Since(start)}

	case "bcrypt":
		output, err := RunBcrypt()
		if err != nil {
			return Result{Err: err, Duration: time.Since(start)}
		}
		return Result{Output: output, Duration: time.Since(start)}

	case "sort":
		output := RunSort()
		return Result{Output: output, Duration: time.Since(start)}

	default:
		// This branch should never be reached in normal operation because
		// job-submitter validates types before enqueuing. If it is reached it
		// means either:
		//   a) A job was pushed to Redis directly (e.g. in a test or migration), or
		//   b) A new type was added to the submitter but not yet to this switch.
		// In both cases, return an explicit error so the job hash is marked
		// "error" in Redis and the submitter's client gets a meaningful status.
		return Result{
			Err:      fmt.Errorf("unknown job type %q: add a case to jobs/dispatcher.go", job.Type),
			Duration: time.Since(start),
		}
	}
}
