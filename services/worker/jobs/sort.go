package jobs

import (
	"fmt"
	"math/rand"
	"sort"
)

// SortSize is the number of int64 elements generated and sorted per job.
//
// Why 500,000 instead of the assignment's 100,000?
// Sorting 100k int64s in Go takes ~5–8ms — barely measurable CPU-wise.
// At 500k elements the sort takes ~60–100ms, which combined with the memory
// allocation and random number generation brings the total job duration to
// ~80–150ms. This is enough to meaningfully contribute to CPU pressure when
// many jobs are queued simultaneously.
//
// The sort job is the fastest of the three. Its role in the demo is to show
// high throughput (many jobs/second) rather than deep CPU saturation per job.
// A mixed workload of prime + bcrypt + sort gives the Grafana histogram a
// realistic multi-modal distribution.
const SortSize = 500_000

// RunSort allocates a slice of SortSize random int64s, sorts it, and returns
// the minimum (first) element as a result string.
//
// Why int64 instead of int?
// On arm64 (Apple Silicon, AWS Graviton) int and int64 are both 64-bit, but
// using int64 explicitly ensures the workload is identical on any target
// architecture — important because the final container runs on amd64 inside
// Minikube/KinD even when built on arm64.
func RunSort() string {
	// Seed with a fixed value so the workload is reproducible across runs.
	// A fixed seed means this job is deterministic: same result every time,
	// which simplifies debugging ("result should always be X").
	//
	// rand.New(rand.NewSource(42)) is used instead of the global rand functions
	// to avoid contention on the global mutex under concurrent load. Each
	// goroutine that calls RunSort gets its own independent RNG state.
	rng := rand.New(rand.NewSource(42))

	data := make([]int64, SortSize)
	for i := range data {
		data[i] = rng.Int63()
	}

	sort.Slice(data, func(i, j int) bool { return data[i] < data[j] })

	return fmt.Sprintf("sorted %d elements, min=%d", SortSize, data[0])
}
