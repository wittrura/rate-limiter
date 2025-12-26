package middleware

import (
	"net/http"
	"strconv"
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

func NewRateLimitWithInfo(kl *limiter.KeyedLimiterWithInfo, keyFunc func(*http.Request) string, now func() time.Time) func(http.Handler) http.Handler {
	if kl == nil {
		panic("rate limiting middleware requires valid limiter")
	}

	if keyFunc == nil {
		panic("rate limiting middleware requires non-nil key function")
	}

	if now == nil {
		now = time.Now
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFunc(r)
			at := now()

			allowed, info := kl.Allow(key, at)
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(info.Limit))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(info.Remaining))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(info.ResetAt.Unix(), 10))

			if !allowed {
				retryAfter := max(info.ResetAt.Sub(at), 0)

				w.Header().Set("Retry-After", strconv.FormatInt(int64(retryAfter.Seconds()), 10))
				http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
