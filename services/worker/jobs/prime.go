// Package jobs contains the CPU-intensive processors for each job type.
// Every processor exposes a single exported Run() function that does the work
// and returns a human-readable result string. No I/O, no side-effects — pure
// computation. This makes each processor trivially testable and benchmarkable.
package jobs

import "fmt"

// PrimeLimit is the upper bound for the Sieve of Eratosthenes.
//
// Why 10,000,000 instead of the assignment's 100,000?
// Go's compiled native code runs the sieve ~10× faster than Node.js on bare
// metal. Benchmarks on Apple Silicon show:
//   - 100k  → ~0.1ms  (original assignment value — useless for HPA)
//   - 1M    → ~2.9ms  (our first attempt — still too fast)
//   - 10M   → ~30ms locally, ~100–200ms inside Colima/Minikube container
//
// At 10M each job takes ~100–200ms of real CPU time inside the container.
// With the worker requests.cpu=200m and the HPA threshold at 70% (140m),
// just two concurrent prime jobs saturate a single pod, causing the HPA to
// trigger scale-out quickly under the ab stress test.
const PrimeLimit = 10_000_000

// RunPrime executes the Sieve of Eratosthenes up to PrimeLimit and returns
// the count of primes found as a formatted string.
//
// Algorithm choice: the Sieve is ideal here because:
//   - It is cache-unfriendly at large limits (bool slice walks memory linearly,
//     thrashing L2/L3), which is exactly what we need to generate CPU pressure.
//   - Its runtime is O(n log log n) — predictable and deterministic, so every
//     job of type "prime" takes approximately the same time. This makes the
//     histogram quantiles meaningful.
//   - It is a well-understood algorithm so the correctness is easy to reason
//     about in a code review.
func RunPrime() string {
	// Allocate a boolean sieve. sieve[i] == true means i is composite.
	// Using a plain []bool (1 byte per element) rather than a bit-packed
	// representation intentionally increases memory pressure, which contributes
	// to CPU stalls and makes the workload more realistic.
	sieve := make([]bool, PrimeLimit+1)

	// 0 and 1 are not prime.
	sieve[0] = true
	sieve[1] = true

	// Classic sieve inner loop. For each prime p found, mark all multiples of
	// p starting at p² as composite. We only need to iterate up to sqrt(limit)
	// because any composite number n has a prime factor ≤ sqrt(n).
	for p := 2; p*p <= PrimeLimit; p++ {
		if !sieve[p] {
			for multiple := p * p; multiple <= PrimeLimit; multiple += p {
				sieve[multiple] = true
			}
		}
	}

	// Count the primes (sieve[i] == false means i is prime).
	count := 0
	for i := 2; i <= PrimeLimit; i++ {
		if !sieve[i] {
			count++
		}
	}

	return fmt.Sprintf("found %d primes up to %d", count, PrimeLimit)
}
