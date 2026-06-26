// Package middleware provides HTTP middleware for the stats-aggregator service.
package middleware

import (
	"net"
	"net/http"
	"sync"
	"time"
)

type ipLimiter struct {
	mu      sync.Mutex
	count   int
	resetAt time.Time
}

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

var limiters sync.Map

// RateLimit limits each unique source IP to limit requests per window.
func RateLimit(limit int, window time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ip, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				ip = r.RemoteAddr
			}
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
