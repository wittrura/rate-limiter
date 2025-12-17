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

func TestLimiter_ResetsAfterWindowPasses(t *testing.T) {
	t.Parallel()

	maxRequests := 2
	window := 1 * time.Minute
	limiter := NewLimiter(maxRequests, window)

	baseTime := time.Date(2025, time.December, 9, 12, 0, 0, 0, time.UTC)

	// Use up the quota in the first window.
	if !limiter.Allow(baseTime) {
		t.Fatalf("expected first request to be allowed")
	}
	if !limiter.Allow(baseTime.Add(10 * time.Second)) {
		t.Fatalf("expected second request to be allowed")
	}

	// Still within the same window: should be denied.
	if allowed := limiter.Allow(baseTime.Add(20 * time.Second)); allowed {
		t.Fatalf("expected request within window after quota to be denied, but it was allowed")
	}

	// Move just beyond one full window.
	afterWindow := baseTime.Add(window + time.Nanosecond)

	// Quota should reset; first request in new window should be allowed.
	if !limiter.Allow(afterWindow) {
		t.Fatalf("expected request after window reset to be allowed, but it was denied")
	}

	// And we should still enforce the max in the new window.
	if !limiter.Allow(afterWindow.Add(10 * time.Second)) {
		t.Fatalf("expected second request in new window to be allowed, but it was denied")
	}

	// Third in the same new window should be denied.
	if allowed := limiter.Allow(afterWindow.Add(20 * time.Second)); allowed {
		t.Fatalf("expected third request in new window to be denied, but it was allowed")
	}
}

func TestLimiter_WithDifferentConfigsIndependently(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2025, time.December, 9, 13, 0, 0, 0, time.UTC)

	// This test assumes each limiter instance tracks state independently.
	limiterFast := NewLimiter(1, 10*time.Second) // 1 request per 10s
	limiterSlow := NewLimiter(2, time.Minute)    // 2 requests per 60s

	// First call on both should be allowed.
	if !limiterFast.Allow(baseTime) {
		t.Fatalf("expected first request on limiterFast to be allowed")
	}
	if !limiterSlow.Allow(baseTime) {
		t.Fatalf("expected first request on limiterSlow to be allowed")
	}

	// Second call within window:
	if limiterFast.Allow(baseTime.Add(5 * time.Second)) {
		t.Fatalf("expected second request on limiterFast within window to be denied")
	}
	if !limiterSlow.Allow(baseTime.Add(5 * time.Second)) {
		t.Fatalf("expected second request on limiterSlow within window to be allowed")
	}

	// Move time forward so limiterFast window has passed but limiterSlow window has not.
	timeAfterFastWindow := baseTime.Add(11 * time.Second)

	if !limiterFast.Allow(timeAfterFastWindow) {
		t.Fatalf("expected limiterFast to reset after its window")
	}

	// limiterSlow still in same window; third request should be denied.
	if limiterSlow.Allow(timeAfterFastWindow) {
		t.Fatalf("expected third request on limiterSlow within same window to be denied")
	}
}

func TestNewLimiter_PanicsOnInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxRequests int
		window      time.Duration
	}{
		{"zero maxRequests", 0, time.Minute},
		{"negative maxRequests", -1, time.Minute},
		{"zero window", 1, 0},
		{"negative window", 1, -1 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected NewLimiter to panic for invalid config: %+v", tt)
				}
			}()

			_ = NewLimiter(tt.maxRequests, tt.window)
		})
	}
}
