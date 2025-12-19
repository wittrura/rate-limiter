package limiter_test

import (
	"sync"
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

func TestRateLimiter_AllowsIndependentlyPerKey(t *testing.T) {
	t.Parallel()

	rl := NewRateLimiter(2, time.Minute)
	base := time.Date(2025, time.December, 9, 14, 0, 0, 0, time.UTC)

	// keyA uses up its quota
	if !rl.Allow("keyA", base) {
		t.Fatalf("expected keyA first request allowed")
	}
	if !rl.Allow("keyA", base.Add(1*time.Second)) {
		t.Fatalf("expected keyA second request allowed")
	}
	if rl.Allow("keyA", base.Add(2*time.Second)) {
		t.Fatalf("expected keyA third request denied within window")
	}

	// keyB should be unaffected
	if !rl.Allow("keyB", base.Add(2*time.Second)) {
		t.Fatalf("expected keyB first request allowed even though keyA is limited")
	}
	if !rl.Allow("keyB", base.Add(3*time.Second)) {
		t.Fatalf("expected keyB second request allowed")
	}
	if rl.Allow("keyB", base.Add(4*time.Second)) {
		t.Fatalf("expected keyB third request denied within window")
	}
}

func TestRateLimiter_ResetsPerKeyIndependently(t *testing.T) {
	t.Parallel()

	rl := NewRateLimiter(1, 10*time.Second)
	base := time.Date(2025, time.December, 9, 14, 30, 0, 0, time.UTC)

	// Both keys allowed once in their initial window
	if !rl.Allow("keyA", base) {
		t.Fatalf("expected keyA first request allowed")
	}
	if !rl.Allow("keyB", base) {
		t.Fatalf("expected keyB first request allowed")
	}

	// Second within same window denied for both
	if rl.Allow("keyA", base.Add(5*time.Second)) {
		t.Fatalf("expected keyA second request denied within window")
	}
	if rl.Allow("keyB", base.Add(5*time.Second)) {
		t.Fatalf("expected keyB second request denied within window")
	}

	// Advance time: keyA resets, but keyB makes no calls until later — both should reset based on their own windowStart.
	afterWindow := base.Add(10*time.Second + time.Nanosecond)

	if !rl.Allow("keyA", afterWindow) {
		t.Fatalf("expected keyA request after window allowed (reset)")
	}
	if !rl.Allow("keyB", afterWindow) {
		t.Fatalf("expected keyB request after window allowed (reset)")
	}
}

func TestRateLimiter_TreatsEmptyKeyAsAKey(t *testing.T) {
	t.Parallel()

	rl := NewRateLimiter(1, time.Minute)
	base := time.Date(2025, time.December, 9, 15, 0, 0, 0, time.UTC)

	if !rl.Allow("", base) {
		t.Fatalf("expected empty key first request allowed")
	}
	if rl.Allow("", base.Add(1*time.Second)) {
		t.Fatalf("expected empty key second request denied within window")
	}

	// Another key should be independent
	if !rl.Allow("x", base.Add(1*time.Second)) {
		t.Fatalf("expected non-empty key allowed independently of empty key")
	}
}

func TestNewRateLimiter_PanicsOnInvalidConfig(t *testing.T) {
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
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected NewRateLimiter to panic for invalid config: %+v", tt)
				}
			}()

			_ = NewRateLimiter(tt.maxRequests, tt.window)
		})
	}
}

func TestRateLimiter_CreatesStateLazilyForNewKeys(t *testing.T) {
	t.Parallel()

	rl := NewRateLimiter(2, time.Minute)
	base := time.Date(2025, time.December, 9, 15, 30, 0, 0, time.UTC)

	// No prior setup for this key; first calls should work.
	if !rl.Allow("brandNew", base) {
		t.Fatalf("expected first request for new key allowed")
	}
	if !rl.Allow("brandNew", base.Add(1*time.Second)) {
		t.Fatalf("expected second request for new key allowed")
	}
	if rl.Allow("brandNew", base.Add(2*time.Second)) {
		t.Fatalf("expected third request for new key denied")
	}
}

func TestRateLimiter_ConcurrentSameKey_AllowsAtMostMax(t *testing.T) {
	t.Parallel()

	const (
		maxRequests = 50
		window      = time.Minute
		goroutines  = 200
	)

	rl := NewRateLimiter(maxRequests, window)
	at := time.Date(2025, time.December, 9, 16, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	allowedCh := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			allowedCh <- rl.Allow("same", at)
		}()
	}

	wg.Wait()
	close(allowedCh)

	allowedCount := 0
	for a := range allowedCh {
		if a {
			allowedCount++
		}
	}

	if allowedCount != maxRequests {
		t.Fatalf("expected exactly %d allowed, got %d", maxRequests, allowedCount)
	}
}

func TestRateLimiter_ConcurrentDifferentKeys_DoNotInterfere(t *testing.T) {
	t.Parallel()

	const (
		maxRequests = 10
		window      = time.Minute
		keys        = 40
		perKeyCalls = 50
	)

	rl := NewRateLimiter(maxRequests, window)
	at := time.Date(2025, time.December, 9, 16, 30, 0, 0, time.UTC)

	var wg sync.WaitGroup
	wg.Add(keys * perKeyCalls)

	type result struct {
		key     string
		allowed bool
	}
	results := make(chan result, keys*perKeyCalls)

	for k := 0; k < keys; k++ {
		key := "key-" + string(rune('A'+k)) // deterministic, simple
		for i := 0; i < perKeyCalls; i++ {
			go func(key string) {
				defer wg.Done()
				results <- result{key: key, allowed: rl.Allow(key, at)}
			}(key)
		}
	}

	wg.Wait()
	close(results)

	allowedPerKey := make(map[string]int)
	for r := range results {
		if r.allowed {
			allowedPerKey[r.key]++
		}
	}

	// Each key should allow exactly maxRequests and deny the rest.
	for k := 0; k < keys; k++ {
		key := "key-" + string(rune('A'+k))
		if allowedPerKey[key] != maxRequests {
			t.Fatalf("expected key %q to allow %d, got %d", key, maxRequests, allowedPerKey[key])
		}
	}
}

func TestRateLimiter_ConcurrentLazyInit_DoesNotPanicOrRace(t *testing.T) {
	t.Parallel()

	rl := NewRateLimiter(1, time.Minute)
	at := time.Date(2025, time.December, 9, 17, 0, 0, 0, time.UTC)

	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// All goroutines contend on a brand new key at once.
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = rl.Allow("brand-new-key", at)
		}()
	}

	wg.Wait()
}
