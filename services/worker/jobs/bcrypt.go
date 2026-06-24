package jobs

import (
	"fmt"

	"golang.org/x/crypto/bcrypt"
)

// BcryptCost is the work factor passed to bcrypt.GenerateFromPassword.
//
// Why 14 instead of the assignment's 10?
// bcrypt cost is exponential: each increment doubles the work.
// cost=10 → ~100ms on modern hardware (Node.js baseline)
// cost=11 → ~200ms
// cost=12 → ~400ms
// cost=13 → ~800ms
// cost=14 → ~1.6s   ← chosen value
//
// Go's bcrypt implementation is written in pure Go (no cgo), which means it
// runs at roughly the same speed as the reference C implementation. At cost=14
// each job burns approximately 1.5–2 seconds of a single CPU core, making it
// the most reliably CPU-saturating job type in this suite. Under 200 concurrent
// ab requests, even 2 worker pods will max their CPU in under 10 seconds.
const BcryptCost = 14

// bcryptInput is the fixed plaintext used for every bcrypt job.
// Using a constant value is intentional: it removes randomness from the
// workload, making benchmark comparisons between runs meaningful.
const bcryptInput = "kubernetes-microservices-monitoring-assignment"

// RunBcrypt hashes bcryptInput with the configured cost and returns the
// resulting hash string. The hash itself is discarded after this function
// returns — the work is the point, not the output.
func RunBcrypt() (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(bcryptInput), BcryptCost)
	if err != nil {
		return "", fmt.Errorf("bcrypt.GenerateFromPassword cost=%d: %w", BcryptCost, err)
	}
	return fmt.Sprintf("bcrypt cost=%d hash=%s", BcryptCost, string(hash)), nil
}
