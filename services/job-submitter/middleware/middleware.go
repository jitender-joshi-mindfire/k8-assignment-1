// Package middleware provides HTTP middleware for the job-submitter service.
package middleware

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// corsMiddleware sets permissive CORS headers for browser clients that call the
// API directly. In Kubernetes the Ingress controller sits in front, but CORS
// headers must still originate from the application layer.
func CORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Request-ID")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// ipLimiter tracks a per-IP sliding-window counter.
type ipLimiter struct {
	mu      sync.Mutex
	count   int
	resetAt time.Time
}

// allow returns true if this IP has not exceeded the per-window limit.
func (l *ipLimiter) allow(limit int, window time.Duration) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if now.After(l.resetAt) {
		l.count = 0
		l.resetAt = now.Add(window)
	}
	l.count++
	return l.count <= limit
}

var (
	limiters sync.Map // map[string]*ipLimiter
)

// RateLimit returns a middleware that allows at most limit requests per window
// from each unique client IP. Excess requests receive 429 Too Many Requests.
//
// limiters are stored in a sync.Map and never evicted during the process
// lifetime — acceptable for a cluster service where the number of distinct
// source IPs is bounded by Kubernetes node/pod count.
func RateLimit(limit int, window time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ip = r.RemoteAddr
			}
			// Check X-Real-IP set by ingress controller.
			if real := r.Header.Get("X-Real-Ip"); real != "" {
				ip = real
			}

			v, _ := limiters.LoadOrStore(ip, &ipLimiter{resetAt: time.Now().Add(window)})
			lim := v.(*ipLimiter)
			if !lim.allow(limit, window) {
				http.Error(w, `{"error":"rate limit exceeded"}`, http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
