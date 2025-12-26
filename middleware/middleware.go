package middleware

import (
	"net/http"
	"time"

	"example.com/rate-limiter/limiter"
)

func NewRateLimit(kl *limiter.KeyedLimiter, keyFunc func(*http.Request) string) func(http.Handler) http.Handler {
	if kl == nil {
		panic("rate limiting middleware requires valid limiter")
	}

	if keyFunc == nil {
		panic("rate limiting middleware requires non-nil key function")
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFunc(r)
			at := time.Now()

			if !kl.Allow(key, at) {
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
