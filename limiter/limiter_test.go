package limiter_test

import (
	"testing"
	"time"

	. "example.com/rate-limiter/limiter"
)

func TestLimiter_AllowsUpToMaxInWindow(t *testing.T) {
	t.Parallel()

	maxRequests := 3
	window := 1 * time.Minute
	limiter := NewLimiter(maxRequests, window)

	baseTime := time.Date(2025, time.December, 9, 10, 0, 0, 0, time.UTC)

	for i := range maxRequests {
		allowed := limiter.Allow(baseTime.Add(time.Duration(i) * time.Second))
		if !allowed {
			t.Fatalf("expected request %d to be allowed, but it was denied", i+1)
		}
	}
}

func TestLimiter_BlocksWhenOverLimitWithinWindow(t *testing.T) {
	t.Parallel()

	maxRequests := 3
	window := 1 * time.Minute
	limiter := NewLimiter(maxRequests, window)

	baseTime := time.Date(2025, time.December, 9, 11, 0, 0, 0, time.UTC)

	// First three requests within the same window should be allowed.
	for i := range maxRequests {
		allowed := limiter.Allow(baseTime.Add(time.Duration(i) * time.Second))
		if !allowed {
			t.Fatalf("expected request %d to be allowed, but it was denied", i+1)
		}
	}

	// Fourth request within the same window should be denied.
	fourthTime := baseTime.Add(30 * time.Second)
	if allowed := limiter.Allow(fourthTime); allowed {
		t.Fatalf("expected fourth request in window to be denied, but it was allowed")
	}
}
