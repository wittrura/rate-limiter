package limiter_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "example.com/rate-limiter/limiter"
)

func TestFixedWindow_AllowsUpToMaxInWindow(t *testing.T) {
	t.Parallel()

	maxRequests := 3
	window := 1 * time.Minute
	limiter := NewFixedWindow(maxRequests, window)

	baseTime := time.Date(2025, time.December, 9, 10, 0, 0, 0, time.UTC)

	for i := range maxRequests {
		allowed := limiter.Allow(baseTime.Add(time.Duration(i) * time.Second))
		if !allowed {
			t.Fatalf("expected request %d to be allowed, but it was denied", i+1)
		}
	}
}

func TestFixedWindow_BlocksWhenOverLimitWithinWindow(t *testing.T) {
	t.Parallel()

	maxRequests := 3
	window := 1 * time.Minute
	limiter := NewFixedWindow(maxRequests, window)

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

func TestFixedWindow_ResetsAfterWindowPasses(t *testing.T) {
	t.Parallel()

	maxRequests := 2
	window := 1 * time.Minute
	limiter := NewFixedWindow(maxRequests, window)

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

func TestFixedWindow_WithDifferentConfigsIndependently(t *testing.T) {
	t.Parallel()

	baseTime := time.Date(2025, time.December, 9, 13, 0, 0, 0, time.UTC)

	// This test assumes each limiter instance tracks state independently.
	limiterFast := NewFixedWindow(1, 10*time.Second) // 1 request per 10s
	limiterSlow := NewFixedWindow(2, time.Minute)    // 2 requests per 60s

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

func TestNewFixedWindow_PanicsOnInvalidConfig(t *testing.T) {
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

			_ = NewFixedWindow(tt.maxRequests, tt.window)
		})
	}
}

func TestFixedWindowLimiter_AllowsIndependentlyPerKey(t *testing.T) {
	t.Parallel()

	rl := NewFixedWindowLimiter(2, time.Minute)
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

func TestFixedWindowLimiter_ResetsPerKeyIndependently(t *testing.T) {
	t.Parallel()

	rl := NewFixedWindowLimiter(1, 10*time.Second)
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

func TestFixedWindowLimiter_TreatsEmptyKeyAsAKey(t *testing.T) {
	t.Parallel()

	rl := NewFixedWindowLimiter(1, time.Minute)
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

func TestNewFixedWindowLimiter_PanicsOnInvalidConfig(t *testing.T) {
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
					t.Fatalf("expected NewFixedWindowLimiter to panic for invalid config: %+v", tt)
				}
			}()

			_ = NewFixedWindowLimiter(tt.maxRequests, tt.window)
		})
	}
}

func TestFixedWindowLimiter_CreatesStateLazilyForNewKeys(t *testing.T) {
	t.Parallel()

	rl := NewFixedWindowLimiter(2, time.Minute)
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

func TestFixedWindowLimiter_ConcurrentSameKey_AllowsAtMostMax(t *testing.T) {
	t.Parallel()

	const (
		maxRequests = 50
		window      = time.Minute
		goroutines  = 200
	)

	rl := NewFixedWindowLimiter(maxRequests, window)
	at := time.Date(2025, time.December, 9, 16, 0, 0, 0, time.UTC)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	allowedCh := make(chan bool, goroutines)

	for range goroutines {
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

func TestFixedWindowLimiter_ConcurrentDifferentKeys_DoNotInterfere(t *testing.T) {
	t.Parallel()

	const (
		maxRequests = 10
		window      = time.Minute
		keys        = 40
		perKeyCalls = 50
	)

	rl := NewFixedWindowLimiter(maxRequests, window)
	at := time.Date(2025, time.December, 9, 16, 30, 0, 0, time.UTC)

	var wg sync.WaitGroup
	wg.Add(keys * perKeyCalls)

	type result struct {
		key     string
		allowed bool
	}
	results := make(chan result, keys*perKeyCalls)

	for k := range keys {
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

func TestFixedWindowLimiter_ConcurrentLazyInit_DoesNotPanicOrRace(t *testing.T) {
	t.Parallel()

	rl := NewFixedWindowLimiter(1, time.Minute)
	at := time.Date(2025, time.December, 9, 17, 0, 0, 0, time.UTC)

	const goroutines = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// All goroutines contend on a brand new key at once.
	for range goroutines {
		go func() {
			defer wg.Done()
			_ = rl.Allow("brand-new-key", at)
		}()
	}

	wg.Wait()
}

func TestTokenBucket_StartsFull_AllowsUpToCapacityImmediately(t *testing.T) {
	t.Parallel()

	tb := NewTokenBucket(3, time.Second)
	base := time.Date(2025, time.December, 9, 18, 0, 0, 0, time.UTC)

	for i := range 3 {
		if !tb.Allow(base) {
			t.Fatalf("expected call %d to be allowed at start (bucket starts full)", i+1)
		}
	}

	if tb.Allow(base) {
		t.Fatalf("expected 4th call to be denied (capacity exhausted)")
	}
}

func TestTokenBucket_RefillsOneTokenPerInterval(t *testing.T) {
	t.Parallel()

	tb := NewTokenBucket(2, 10*time.Second)
	base := time.Date(2025, time.December, 9, 18, 10, 0, 0, time.UTC)

	// Drain bucket (2 tokens).
	if !tb.Allow(base) {
		t.Fatalf("expected first call allowed")
	}
	if !tb.Allow(base) {
		t.Fatalf("expected second call allowed")
	}
	if tb.Allow(base) {
		t.Fatalf("expected third call denied (bucket empty)")
	}

	// Not enough time for a refill.
	if tb.Allow(base.Add(9 * time.Second)) {
		t.Fatalf("expected denied before refill interval elapses")
	}

	// Exactly one interval later: 1 token should be available.
	if !tb.Allow(base.Add(10 * time.Second)) {
		t.Fatalf("expected allowed after one refill interval")
	}

	// Consumed the refilled token.
	if tb.Allow(base.Add(10 * time.Second)) {
		t.Fatalf("expected denied after consuming the single refilled token")
	}
}

func TestTokenBucket_RefillAccumulatesOverMultipleIntervalsUpToCapacity(t *testing.T) {
	t.Parallel()

	tb := NewTokenBucket(5, 2*time.Second)
	base := time.Date(2025, time.December, 9, 18, 20, 0, 0, time.UTC)

	// Drain all 5 tokens.
	for i := range 5 {
		if !tb.Allow(base) {
			t.Fatalf("expected drain call %d allowed", i+1)
		}
	}
	if tb.Allow(base) {
		t.Fatalf("expected denied after draining capacity")
	}

	// After 7 seconds with a 2s refill interval, we should have refilled floor(7/2)=3 tokens.
	refillTime := base.Add(7 * time.Second)

	for i := range 3 {
		if !tb.Allow(refillTime) {
			t.Fatalf("expected refilled token %d to be available", i+1)
		}
	}

	if tb.Allow(refillTime) {
		t.Fatalf("expected denied after consuming the 3 refilled tokens")
	}

	// After a long time, bucket should cap at capacity, not exceed it.
	longLater := base.Add(60 * time.Second)

	for i := range 5 {
		if !tb.Allow(longLater) {
			t.Fatalf("expected token %d allowed after long refill (capped at capacity)", i+1)
		}
	}
	if tb.Allow(longLater) {
		t.Fatalf("expected denied after consuming capped capacity")
	}
}

func TestTokenBucket_DoesNotGoBackwardInTime(t *testing.T) {
	t.Parallel()

	tb := NewTokenBucket(2, 10*time.Second)
	base := time.Date(2025, time.December, 9, 18, 30, 0, 0, time.UTC)

	// Drain 2 tokens.
	if !tb.Allow(base) || !tb.Allow(base) {
		t.Fatalf("expected initial tokens allowed")
	}
	if tb.Allow(base) {
		t.Fatalf("expected denied when empty")
	}

	// If time goes backward, we should NOT "negative refill" or break state.
	// It should behave as if no refill happened.
	if tb.Allow(base.Add(-5 * time.Second)) {
		t.Fatalf("expected denied when time goes backward and bucket is empty")
	}

	// Normal forward time refill still works.
	if !tb.Allow(base.Add(10 * time.Second)) {
		t.Fatalf("expected allowed after one refill interval")
	}
}

func TestNewTokenBucket_PanicsOnInvalidConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxTokens   int
		refillEvery time.Duration
	}{
		{"zero maxTokens", 0, time.Second},
		{"negative maxTokens", -1, time.Second},
		{"zero refillEvery", 1, 0},
		{"negative refillEvery", 1, -1 * time.Second},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected NewTokenBucket to panic for invalid config: %+v", tt)
				}
			}()

			_ = NewTokenBucket(tt.maxTokens, tt.refillEvery)
		})
	}
}

type fakeStrategy struct {
	remaining int64
}

func newFakeStrategy(allows int) *fakeStrategy {
	return &fakeStrategy{remaining: int64(allows)}
}

func (s *fakeStrategy) Allow(_ time.Time) bool {
	for {
		cur := atomic.LoadInt64(&s.remaining)
		if cur <= 0 {
			return false
		}

		if atomic.CompareAndSwapInt64(&s.remaining, cur, cur-1) {
			return true
		}
	}
}

func TestNewKeyedLimiter_PanicsOnNilFactory(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected NewKeyedLimiter to panic on nil factory")
		}
	}()

	_ = NewKeyedLimiter(nil)
}

func TestKeyedLimiter_LazilyCreatesStrategyPerNewKey(t *testing.T) {
	t.Parallel()

	var created int32
	factory := func() Strategy {
		atomic.AddInt32(&created, 1)
		return newFakeStrategy(1)
	}

	kl := NewKeyedLimiter(factory)
	at := time.Date(2025, time.December, 9, 19, 0, 0, 0, time.UTC)

	// No keys touched yet.
	if atomic.LoadInt32(&created) != 0 {
		t.Fatalf("expected no strategies created before first Allow call")
	}

	if !kl.Allow("a", at) {
		t.Fatalf("expected key a allowed (fresh strategy)")
	}
	if atomic.LoadInt32(&created) != 1 {
		t.Fatalf("expected 1 strategy created after first new key")
	}

	if !kl.Allow("b", at) {
		t.Fatalf("expected key b allowed (fresh strategy)")
	}
	if atomic.LoadInt32(&created) != 2 {
		t.Fatalf("expected 2 strategies created after second new key")
	}
}

func TestKeyedLimiter_ReusesSameStrategyForSameKey(t *testing.T) {
	t.Parallel()

	var created int32
	factory := func() Strategy {
		atomic.AddInt32(&created, 1)
		return newFakeStrategy(2)
	}

	kl := NewKeyedLimiter(factory)
	at := time.Date(2025, time.December, 9, 19, 10, 0, 0, time.UTC)

	// Same key should not create new strategy instances.
	if !kl.Allow("same", at) {
		t.Fatalf("expected first call allowed")
	}
	if !kl.Allow("same", at) {
		t.Fatalf("expected second call allowed")
	}
	if kl.Allow("same", at) {
		t.Fatalf("expected third call denied")
	}

	if atomic.LoadInt32(&created) != 1 {
		t.Fatalf("expected exactly 1 strategy instance created for the key, got %d", atomic.LoadInt32(&created))
	}
}

func TestKeyedLimiter_DifferentKeysUseIndependentStrategyState(t *testing.T) {
	t.Parallel()

	factory := func() Strategy {
		// Each key gets a strategy that allows exactly 1 request.
		return newFakeStrategy(1)
	}

	kl := NewKeyedLimiter(factory)
	at := time.Date(2025, time.December, 9, 19, 20, 0, 0, time.UTC)

	// keyA
	if !kl.Allow("keyA", at) {
		t.Fatalf("expected keyA first allowed")
	}
	if kl.Allow("keyA", at) {
		t.Fatalf("expected keyA second denied")
	}

	// keyB should be independent
	if !kl.Allow("keyB", at) {
		t.Fatalf("expected keyB first allowed")
	}
	if kl.Allow("keyB", at) {
		t.Fatalf("expected keyB second denied")
	}
}

func TestKeyedLimiter_TreatsEmptyKeyAsAKey(t *testing.T) {
	t.Parallel()

	factory := func() Strategy {
		return newFakeStrategy(1)
	}

	kl := NewKeyedLimiter(factory)
	at := time.Date(2025, time.December, 9, 19, 30, 0, 0, time.UTC)

	if !kl.Allow("", at) {
		t.Fatalf("expected empty key first allowed")
	}
	if kl.Allow("", at) {
		t.Fatalf("expected empty key second denied")
	}

	// Another key should be independent
	if !kl.Allow("x", at) {
		t.Fatalf("expected non-empty key allowed independently of empty key")
	}
}

func TestKeyedLimiter_ConcurrentFirstUseSameKey_CreatesOnlyOneStrategy(t *testing.T) {
	t.Parallel()

	var created int32
	factory := func() Strategy {
		atomic.AddInt32(&created, 1)
		return newFakeStrategy(1000)
	}

	kl := NewKeyedLimiter(factory)
	at := time.Date(2025, time.December, 9, 19, 40, 0, 0, time.UTC)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			_ = kl.Allow("brand-new", at)
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&created) != 1 {
		t.Fatalf("expected exactly 1 strategy created under concurrent first use, got %d", atomic.LoadInt32(&created))
	}
}

type fakeStrategyWithInfo struct {
	limit     int
	remaining int64
	resetAt   time.Time
}

func newFakeStrategyWithInfo(limit int, allows int, resetAt time.Time) *fakeStrategyWithInfo {
	return &fakeStrategyWithInfo{
		limit:     limit,
		remaining: int64(allows),
		resetAt:   resetAt,
	}
}

func (s *fakeStrategyWithInfo) Allow(_ time.Time) (bool, Info) {
	for {
		cur := atomic.LoadInt64(&s.remaining)
		if cur <= 0 {
			return false, Info{
				Limit:     s.limit,
				Remaining: 0,
				ResetAt:   s.resetAt,
			}
		}
		if atomic.CompareAndSwapInt64(&s.remaining, cur, cur-1) {
			rem := int(cur - 1)
			if rem < 0 {
				rem = 0
			}
			return true, Info{
				Limit:     s.limit,
				Remaining: rem,
				ResetAt:   s.resetAt,
			}
		}
	}
}

func TestNewKeyedLimiterWithInfo_PanicsOnNilFactory(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected NewKeyedLimiterWithInfo to panic on nil factory")
		}
	}()

	_ = NewKeyedLimiterWithInfo(nil)
}

func TestKeyedLimiterWithInfo_LazilyCreatesStrategyPerNewKey(t *testing.T) {
	t.Parallel()

	var created int32
	resetAt := time.Date(2025, time.December, 9, 20, 0, 0, 0, time.UTC)

	factory := func() StrategyWithInfo {
		atomic.AddInt32(&created, 1)
		return newFakeStrategyWithInfo(1, 1, resetAt)
	}

	kl := NewKeyedLimiterWithInfo(factory)
	at := time.Date(2025, time.December, 9, 19, 50, 0, 0, time.UTC)

	if atomic.LoadInt32(&created) != 0 {
		t.Fatalf("expected no strategies created before first Allow")
	}

	allowedA, infoA := kl.Allow("a", at)
	if !allowedA {
		t.Fatalf("expected key a allowed")
	}
	if infoA.Limit != 1 || infoA.Remaining != 0 || !infoA.ResetAt.Equal(resetAt) {
		t.Fatalf("unexpected info for key a: %+v", infoA)
	}

	if atomic.LoadInt32(&created) != 1 {
		t.Fatalf("expected 1 strategy created after first new key, got %d", atomic.LoadInt32(&created))
	}

	allowedB, _ := kl.Allow("b", at)
	if !allowedB {
		t.Fatalf("expected key b allowed")
	}

	if atomic.LoadInt32(&created) != 2 {
		t.Fatalf("expected 2 strategies created after second new key, got %d", atomic.LoadInt32(&created))
	}
}

func TestKeyedLimiterWithInfo_ReusesSameStrategyForSameKey(t *testing.T) {
	t.Parallel()

	var created int32
	resetAt := time.Date(2025, time.December, 9, 20, 10, 0, 0, time.UTC)

	factory := func() StrategyWithInfo {
		atomic.AddInt32(&created, 1)
		return newFakeStrategyWithInfo(2, 2, resetAt)
	}

	kl := NewKeyedLimiterWithInfo(factory)
	at := time.Date(2025, time.December, 9, 20, 0, 0, 0, time.UTC)

	allowed1, info1 := kl.Allow("same", at)
	if !allowed1 || info1.Limit != 2 || info1.Remaining != 1 {
		t.Fatalf("unexpected result for first allow: allowed=%v info=%+v", allowed1, info1)
	}

	allowed2, info2 := kl.Allow("same", at)
	if !allowed2 || info2.Limit != 2 || info2.Remaining != 0 {
		t.Fatalf("unexpected result for second allow: allowed=%v info=%+v", allowed2, info2)
	}

	allowed3, info3 := kl.Allow("same", at)
	if allowed3 || info3.Limit != 2 || info3.Remaining != 0 {
		t.Fatalf("unexpected result for third allow: allowed=%v info=%+v", allowed3, info3)
	}

	if atomic.LoadInt32(&created) != 1 {
		t.Fatalf("expected exactly 1 strategy created for the key, got %d", atomic.LoadInt32(&created))
	}
	if !info3.ResetAt.Equal(resetAt) {
		t.Fatalf("expected resetAt %v, got %v", resetAt, info3.ResetAt)
	}
}

func TestKeyedLimiterWithInfo_DifferentKeysAreIndependent(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2025, time.December, 9, 20, 20, 0, 0, time.UTC)
	factory := func() StrategyWithInfo {
		return newFakeStrategyWithInfo(1, 1, resetAt)
	}

	kl := NewKeyedLimiterWithInfo(factory)
	at := time.Date(2025, time.December, 9, 20, 15, 0, 0, time.UTC)

	// keyA
	if allowed, _ := kl.Allow("keyA", at); !allowed {
		t.Fatalf("expected keyA first allowed")
	}
	if allowed, _ := kl.Allow("keyA", at); allowed {
		t.Fatalf("expected keyA second denied")
	}

	// keyB independent
	if allowed, _ := kl.Allow("keyB", at); !allowed {
		t.Fatalf("expected keyB first allowed")
	}
	if allowed, _ := kl.Allow("keyB", at); allowed {
		t.Fatalf("expected keyB second denied")
	}
}

func TestKeyedLimiterWithInfo_TreatsEmptyKeyAsAKey(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2025, time.December, 9, 20, 30, 0, 0, time.UTC)
	factory := func() StrategyWithInfo {
		return newFakeStrategyWithInfo(1, 1, resetAt)
	}

	kl := NewKeyedLimiterWithInfo(factory)
	at := time.Date(2025, time.December, 9, 20, 25, 0, 0, time.UTC)

	if allowed, _ := kl.Allow("", at); !allowed {
		t.Fatalf("expected empty key first allowed")
	}
	if allowed, _ := kl.Allow("", at); allowed {
		t.Fatalf("expected empty key second denied")
	}

	if allowed, _ := kl.Allow("x", at); !allowed {
		t.Fatalf("expected non-empty key independent of empty key")
	}
}

func TestKeyedLimiterWithInfo_ConcurrentFirstUseSameKey_CreatesOnlyOneStrategy(t *testing.T) {
	t.Parallel()

	var created int32
	resetAt := time.Date(2025, time.December, 9, 20, 40, 0, 0, time.UTC)

	factory := func() StrategyWithInfo {
		atomic.AddInt32(&created, 1)
		return newFakeStrategyWithInfo(1000, 1000, resetAt)
	}

	kl := NewKeyedLimiterWithInfo(factory)
	at := time.Date(2025, time.December, 9, 20, 35, 0, 0, time.UTC)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for range goroutines {
		go func() {
			defer wg.Done()
			_, _ = kl.Allow("brand-new", at)
		}()
	}

	wg.Wait()

	if atomic.LoadInt32(&created) != 1 {
		t.Fatalf("expected exactly 1 strategy created under concurrent first use, got %d", atomic.LoadInt32(&created))
	}
}

type trackingStrategy struct{}

func (trackingStrategy) Allow(_ time.Time) (bool, Info) {
	return true, Info{
		Limit:     1,
		Remaining: 0,
		ResetAt:   time.Unix(0, 0),
	}
}

func TestNewKeyedLimiterWithInfoAndCleanup_PanicsOnInvalidArgs(t *testing.T) {
	t.Parallel()

	validFactory := func() StrategyWithInfo { return trackingStrategy{} }
	validNow := func() time.Time { return time.Unix(0, 0) }

	t.Run("nil factory", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for nil factory")
			}
		}()

		_ = NewKeyedLimiterWithInfoAndCleanup(nil, CleanupConfig{
			IdleTTL:      time.Second,
			CleanupEvery: 10 * time.Millisecond,
		}, validNow)
	})

	t.Run("nil now", func(t *testing.T) {
		t.Parallel()

		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for nil now")
			}
		}()

		_ = NewKeyedLimiterWithInfoAndCleanup(validFactory, CleanupConfig{
			IdleTTL:      time.Second,
			CleanupEvery: 10 * time.Millisecond,
		}, nil)
	})

	tests := []struct {
		name string
		cfg  CleanupConfig
	}{
		{"zero IdleTTL", CleanupConfig{IdleTTL: 0, CleanupEvery: 10 * time.Millisecond}},
		{"negative IdleTTL", CleanupConfig{IdleTTL: -1 * time.Second, CleanupEvery: 10 * time.Millisecond}},
		{"zero CleanupEvery", CleanupConfig{IdleTTL: time.Second, CleanupEvery: 0}},
		{"negative CleanupEvery", CleanupConfig{IdleTTL: time.Second, CleanupEvery: -1 * time.Millisecond}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for invalid config: %+v", tt.cfg)
				}
			}()

			_ = NewKeyedLimiterWithInfoAndCleanup(validFactory, tt.cfg, validNow)
		})
	}
}

func TestKeyedLimiterWithInfo_CleansUpIdleKeys_RecreatesAfterEviction(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, time.December, 9, 23, 0, 0, 0, time.UTC)
	var currentUnix atomic.Int64
	currentUnix.Store(start.UnixNano())
	cur := func() time.Time { return time.Unix(0, currentUnix.Load()) }
	now := func() time.Time { return cur() }

	var created int32
	factory := func() StrategyWithInfo {
		atomic.AddInt32(&created, 1)
		return trackingStrategy{}
	}

	cfg := CleanupConfig{
		IdleTTL:      50 * time.Millisecond,
		CleanupEvery: 10 * time.Millisecond,
	}

	kl := NewKeyedLimiterWithInfoAndCleanup(factory, cfg, now)
	t.Cleanup(func() { kl.Close() })

	// First use creates strategy for key.
	if allowed, _ := kl.Allow("user-1", cur()); !allowed {
		t.Fatalf("expected allowed")
	}
	if got := atomic.LoadInt32(&created); got != 1 {
		t.Fatalf("expected 1 strategy created, got %d", got)
	}

	// Advance our logical clock beyond IdleTTL.
	currentUnix.Add((200 * time.Millisecond).Nanoseconds())

	// Give cleanup loop time to run at least once.
	time.Sleep(50 * time.Millisecond)

	// If key was evicted, next Allow should recreate.
	if allowed, _ := kl.Allow("user-1", cur()); !allowed {
		t.Fatalf("expected allowed after eviction")
	}
	if got := atomic.LoadInt32(&created); got != 2 {
		t.Fatalf("expected strategy to be recreated after eviction; created=%d", got)
	}
}

func TestKeyedLimiterWithInfo_CloseStopsCleanupAndIsIdempotent(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, time.December, 9, 23, 10, 0, 0, time.UTC)
	var currentUnix atomic.Int64
	currentUnix.Store(start.UnixNano())
	cur := func() time.Time { return time.Unix(0, currentUnix.Load()) }
	now := func() time.Time { return cur() }

	var created int32
	factory := func() StrategyWithInfo {
		atomic.AddInt32(&created, 1)
		return trackingStrategy{}
	}

	cfg := CleanupConfig{
		IdleTTL:      50 * time.Millisecond,
		CleanupEvery: 10 * time.Millisecond,
	}

	kl := NewKeyedLimiterWithInfoAndCleanup(factory, cfg, now)

	// Create key
	if allowed, _ := kl.Allow("user-1", cur()); !allowed {
		t.Fatalf("expected allowed")
	}
	if got := atomic.LoadInt32(&created); got != 1 {
		t.Fatalf("expected 1 strategy created, got %d", got)
	}

	// Close should be safe and stop cleanup; call twice to ensure idempotence.
	kl.Close()
	kl.Close()

	// Advance time beyond TTL.
	currentUnix.Add((200 * time.Millisecond).Nanoseconds())

	// Wait longer than CleanupEvery to ensure cleanup WOULD have run if enabled.
	time.Sleep(50 * time.Millisecond)

	// If cleanup is stopped, the original entry should still exist, so factory should NOT be called again.
	if allowed, _ := kl.Allow("user-1", cur()); !allowed {
		t.Fatalf("expected allowed")
	}
	if got := atomic.LoadInt32(&created); got != 1 {
		t.Fatalf("expected no recreation after Close; created=%d", got)
	}
}

func TestKeyedLimiterWithInfo_CleanupDoesNotEvictRecentlyUsedKey(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, time.December, 9, 23, 20, 0, 0, time.UTC)
	var currentUnix atomic.Int64
	currentUnix.Store(start.UnixNano())
	cur := func() time.Time { return time.Unix(0, currentUnix.Load()) }
	now := func() time.Time { return cur() }

	var created int32
	factory := func() StrategyWithInfo {
		atomic.AddInt32(&created, 1)
		return trackingStrategy{}
	}

	cfg := CleanupConfig{
		IdleTTL:      80 * time.Millisecond,
		CleanupEvery: 10 * time.Millisecond,
	}

	kl := NewKeyedLimiterWithInfoAndCleanup(factory, cfg, now)
	t.Cleanup(func() { kl.Close() })

	// Create key
	if allowed, _ := kl.Allow("hot", cur()); !allowed {
		t.Fatalf("expected allowed")
	}
	if got := atomic.LoadInt32(&created); got != 1 {
		t.Fatalf("expected 1 strategy created, got %d", got)
	}

	// Keep using the key, updating last-access time.
	for range 3 {
		currentUnix.Add((40 * time.Millisecond).Nanoseconds()) // less than IdleTTL
		if allowed, _ := kl.Allow("hot", cur()); !allowed {
			t.Fatalf("expected allowed")
		}
		time.Sleep(15 * time.Millisecond) // allow cleanup ticks
	}

	// If it was not evicted, created should still be 1.
	if got := atomic.LoadInt32(&created); got != 1 {
		t.Fatalf("expected key not evicted while active; created=%d", got)
	}
}

// blockingStrategyWithInfo can block inside Allow() until released.
// It also detects concurrent entry into Allow().
type blockingStrategyWithInfo struct {
	entered     chan struct{} // closed on first entry into Allow
	release     chan struct{} // closing releases Allow to return
	activeCalls int32         // used to detect concurrent entry
	concurrent  int32         // set to 1 if concurrent entry occurs
}

func newBlockingStrategyWithInfo() *blockingStrategyWithInfo {
	return &blockingStrategyWithInfo{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *blockingStrategyWithInfo) Allow(at time.Time) (bool, Info) {
	// Detect concurrent entry.
	if atomic.AddInt32(&s.activeCalls, 1) != 1 {
		atomic.StoreInt32(&s.concurrent, 1)
	}
	defer atomic.AddInt32(&s.activeCalls, -1)

	// Signal entry exactly once.
	select {
	case <-s.entered:
		// already closed
	default:
		close(s.entered)
	}

	// Block until released.
	<-s.release

	return true, Info{
		Limit:     1,
		Remaining: 0,
		ResetAt:   at,
	}
}

func (s *blockingStrategyWithInfo) releaseNow() {
	select {
	case <-s.release:
		// already closed
	default:
		close(s.release)
	}
}

func (s *blockingStrategyWithInfo) hasConcurrentEntry() bool {
	return atomic.LoadInt32(&s.concurrent) == 1
}

func TestKeyedLimiterWithInfo_DifferentKeysDoNotBlockEachOther(t *testing.T) {
	t.Parallel()

	// We want a predictable strategy per key:
	// - key "A" gets a blocking strategy
	// - key "B" gets a non-blocking strategy
	blockA := newBlockingStrategyWithInfo()

	var created int32
	factory := func() StrategyWithInfo {
		// First created is for key A (in this test), second for key B.
		n := atomic.AddInt32(&created, 1)
		if n == 1 {
			return blockA
		}
		// Non-blocking strategy for other keys.
		return StrategyWithInfoFunc(func(at time.Time) (bool, Info) {
			return true, Info{Limit: 1, Remaining: 0, ResetAt: at}
		})
	}

	kl := NewKeyedLimiterWithInfo(factory)

	at := time.Date(2025, time.December, 10, 9, 0, 0, 0, time.UTC)

	// Start key A; it will block inside strategy.Allow.
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = kl.Allow("A", at)
	})

	// Wait until key A has actually entered Allow (so we know it's "stuck").
	select {
	case <-blockA.entered:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for key A to enter strategy.Allow")
	}

	// Now call key B. If KeyedLimiterWithInfo holds the global map lock
	// while calling strategy.Allow, this will block behind A and time out.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = kl.Allow("B", at)
	}()

	select {
	case <-done:
		// Good: different key progressed even while A is blocked.
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("key B call blocked; expected different keys not to contend on global lock during strategy.Allow")
	}

	// Unblock A so goroutine can finish cleanly.
	blockA.releaseNow()
	wg.Wait()
}

func TestKeyedLimiterWithInfo_SerializesCallsForSameKey(t *testing.T) {
	t.Parallel()

	block := newBlockingStrategyWithInfo()
	factory := func() StrategyWithInfo { return block }

	kl := NewKeyedLimiterWithInfo(factory)
	at := time.Date(2025, time.December, 10, 9, 10, 0, 0, time.UTC)

	// First call enters and blocks.
	var wg sync.WaitGroup
	wg.Go(func() {
		_, _ = kl.Allow("same", at)
	})

	// Ensure first call is inside the strategy.
	select {
	case <-block.entered:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for first call to enter strategy.Allow")
	}

	// Second call to same key should NOT enter strategy concurrently.
	secondEntered := make(chan struct{})
	go func() {
		// If implementation is correct, this goroutine will block until we release the first call.
		_, _ = kl.Allow("same", at)
		close(secondEntered)
	}()

	// Give it a moment. If it enters concurrently, our strategy will detect it.
	time.Sleep(50 * time.Millisecond)

	if block.hasConcurrentEntry() {
		t.Fatalf("detected concurrent entry into strategy.Allow for the same key; expected per-key serialization")
	}

	// It also should not have completed yet (because first call still blocked).
	select {
	case <-secondEntered:
		t.Fatalf("second call completed while first call still blocked; expected second call to wait on same key")
	default:
		// Good: still blocked.
	}

	// Release first call; then second should be able to proceed.
	block.releaseNow()

	select {
	case <-secondEntered:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for second call to complete after release")
	}

	wg.Wait()
}

// StrategyWithInfoFunc is a small helper for inline strategies in tests.
// Put it here so we don't have to create new types for tiny behaviors.
type StrategyWithInfoFunc func(at time.Time) (bool, Info)

func (f StrategyWithInfoFunc) Allow(at time.Time) (bool, Info) { return f(at) }

func TestNewShardedKeyedLimiterWithInfo_PanicsOnInvalidArgs(t *testing.T) {
	t.Parallel()

	okFactory := func() StrategyWithInfo { return trackingStrategy{} }
	okHash := func(string) uint64 { return 0 }

	t.Run("nil factory", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for nil factory")
			}
		}()
		_ = NewShardedKeyedLimiterWithInfo(nil, 8, okHash)
	})

	t.Run("invalid shard count", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for shards < 1")
			}
		}()
		_ = NewShardedKeyedLimiterWithInfo(okFactory, 0, okHash)
	})

	t.Run("nil hash", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r == nil {
				t.Fatalf("expected panic for nil hash")
			}
		}()
		_ = NewShardedKeyedLimiterWithInfo(okFactory, 8, nil)
	})
}

func TestShardedKeyedLimiterWithInfo_LazilyCreatesOneStrategyPerKey(t *testing.T) {
	t.Parallel()

	var created int32
	factory := func() StrategyWithInfo {
		atomic.AddInt32(&created, 1)
		return trackingStrategy{}
	}

	// Put everything on shard 0; this test is about correctness, not sharding.
	hash := func(string) uint64 { return 0 }

	kl := NewShardedKeyedLimiterWithInfo(factory, 4, hash)
	at := time.Date(2025, time.December, 10, 10, 0, 0, 0, time.UTC)

	_, _ = kl.Allow("a", at)
	_, _ = kl.Allow("b", at)
	_, _ = kl.Allow("a", at) // reuse

	if got := atomic.LoadInt32(&created); got != 2 {
		t.Fatalf("expected exactly 2 strategies created (one per distinct key), got %d", got)
	}
}

func TestShardedKeyedLimiterWithInfo_DifferentShardsDoNotBlockOnSlowFactory(t *testing.T) {
	t.Parallel()

	// The key property of sharding we want:
	// if one shard is blocked *inside factory creation*, other shards can still proceed.

	factoryEntered := make(chan struct{})
	releaseFactory := make(chan struct{})

	var callN int32
	factory := func() StrategyWithInfo {
		n := atomic.AddInt32(&callN, 1)
		if n == 1 {
			// Block the first creation until the test releases it.
			close(factoryEntered)
			<-releaseFactory
		}
		return trackingStrategy{}
	}

	// Deterministic routing: "A" -> shard 0, "B" -> shard 1
	hash := func(key string) uint64 {
		if key == "A" {
			return 0
		}
		return 1
	}

	kl := NewShardedKeyedLimiterWithInfo(factory, 2, hash)
	at := time.Date(2025, time.December, 10, 10, 10, 0, 0, time.UTC)

	// Start Allow(A) which will block inside factory while holding shard 0 lock.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = kl.Allow("A", at)
	}()

	// Ensure the first factory call is actually blocked.
	select {
	case <-factoryEntered:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for factory to be entered for key A")
	}

	// Now Allow(B) should complete quickly if it hits a different shard lock.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = kl.Allow("B", at)
	}()

	select {
	case <-done:
		// success
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("key B was blocked by slow factory on a different shard; expected sharded locks to isolate contention")
	}

	// Unblock factory so goroutine can finish.
	close(releaseFactory)
	wg.Wait()
}

func TestShardedKeyedLimiterWithInfo_SameShardBlocksOnSlowFactory(t *testing.T) {
	t.Parallel()

	factoryEntered := make(chan struct{})
	releaseFactory := make(chan struct{})

	var callN int32
	factory := func() StrategyWithInfo {
		n := atomic.AddInt32(&callN, 1)
		if n == 1 {
			close(factoryEntered)
			<-releaseFactory
		}
		return trackingStrategy{}
	}

	// Force both keys to same shard (0).
	hash := func(string) uint64 { return 0 }

	kl := NewShardedKeyedLimiterWithInfo(factory, 2, hash)
	at := time.Date(2025, time.December, 10, 10, 20, 0, 0, time.UTC)

	// Block first key creation inside factory under shard 0 lock.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = kl.Allow("A", at)
	}()

	select {
	case <-factoryEntered:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for factory to be entered for key A")
	}

	// Now key B should be blocked, because it goes to the same shard lock.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = kl.Allow("B", at)
	}()

	select {
	case <-done:
		t.Fatalf("expected key B to be blocked when routed to the same shard as A")
	case <-time.After(75 * time.Millisecond):
		// good: still blocked
	}

	close(releaseFactory)
	wg.Wait()

	// After release, B should complete.
	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timed out waiting for key B to complete after releasing factory")
	}
}

func TestNewShardedKeyedLimiterWithInfoAndCleanup_PanicsOnInvalidArgs(t *testing.T) {
	t.Parallel()

	okFactory := func() StrategyWithInfo { return trackingStrategy{} }
	okHash := func(string) uint64 { return 0 }
	okNow := func() time.Time { return time.Unix(0, 0) }
	okCfg := CleanupConfig{IdleTTL: time.Second, CleanupEvery: 10 * time.Millisecond}

	t.Run("nil factory", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic")
			}
		}()
		_ = NewShardedKeyedLimiterWithInfoAndCleanup(nil, 2, okHash, okCfg, okNow)
	})

	t.Run("invalid shards", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic")
			}
		}()
		_ = NewShardedKeyedLimiterWithInfoAndCleanup(okFactory, 0, okHash, okCfg, okNow)
	})

	t.Run("nil hash", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic")
			}
		}()
		_ = NewShardedKeyedLimiterWithInfoAndCleanup(okFactory, 2, nil, okCfg, okNow)
	})

	t.Run("nil now", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if recover() == nil {
				t.Fatalf("expected panic")
			}
		}()
		_ = NewShardedKeyedLimiterWithInfoAndCleanup(okFactory, 2, okHash, okCfg, nil)
	})

	t.Run("invalid cfg", func(t *testing.T) {
		t.Parallel()
		bad := []CleanupConfig{
			{IdleTTL: 0, CleanupEvery: 10 * time.Millisecond},
			{IdleTTL: -1 * time.Second, CleanupEvery: 10 * time.Millisecond},
			{IdleTTL: time.Second, CleanupEvery: 0},
			{IdleTTL: time.Second, CleanupEvery: -1 * time.Millisecond},
		}
		for _, cfg := range bad {
			cfg := cfg
			t.Run(cfg.IdleTTL.String()+"_"+cfg.CleanupEvery.String(), func(t *testing.T) {
				t.Parallel()
				defer func() {
					if recover() == nil {
						t.Fatalf("expected panic for cfg=%+v", cfg)
					}
				}()
				_ = NewShardedKeyedLimiterWithInfoAndCleanup(okFactory, 2, okHash, cfg, okNow)
			})
		}
	})
}

func TestShardedKeyedLimiterWithInfoAndCleanup_EvictsIdleKeysAcrossShards(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, time.December, 10, 11, 0, 0, 0, time.UTC)
	var currentUnix atomic.Int64
	currentUnix.Store(start.UnixNano())
	cur := func() time.Time { return time.Unix(0, currentUnix.Load()) }
	now := func() time.Time { return cur() }

	var created int32
	factory := func() StrategyWithInfo {
		atomic.AddInt32(&created, 1)
		return trackingStrategy{}
	}

	// Route keyA -> shard 0, keyB -> shard 1
	hash := func(key string) uint64 {
		if key == "A" {
			return 0
		}
		return 1
	}

	cfg := CleanupConfig{
		IdleTTL:      50 * time.Millisecond,
		CleanupEvery: 10 * time.Millisecond,
	}

	kl := NewShardedKeyedLimiterWithInfoAndCleanup(factory, 2, hash, cfg, now)
	t.Cleanup(func() { kl.Close() })

	// Touch both keys (two strategies created across two shards).
	if allowed, _ := kl.Allow("A", cur()); !allowed {
		t.Fatalf("expected allowed")
	}
	if allowed, _ := kl.Allow("B", cur()); !allowed {
		t.Fatalf("expected allowed")
	}
	if got := atomic.LoadInt32(&created); got != 2 {
		t.Fatalf("expected 2 strategies created, got %d", got)
	}

	// Advance time beyond TTL so both should be evicted.
	currentUnix.Add((200 * time.Millisecond).Nanoseconds())
	time.Sleep(50 * time.Millisecond)

	// Next uses should recreate both.
	if allowed, _ := kl.Allow("A", cur()); !allowed {
		t.Fatalf("expected allowed after eviction")
	}
	if allowed, _ := kl.Allow("B", cur()); !allowed {
		t.Fatalf("expected allowed after eviction")
	}
	if got := atomic.LoadInt32(&created); got != 4 {
		t.Fatalf("expected both keys recreated after eviction; created=%d", got)
	}
}

func TestShardedKeyedLimiterWithInfoAndCleanup_CloseStopsCleanup(t *testing.T) {
	t.Parallel()

	start := time.Date(2025, time.December, 10, 11, 10, 0, 0, time.UTC)
	var currentUnix atomic.Int64
	currentUnix.Store(start.UnixNano())
	cur := func() time.Time { return time.Unix(0, currentUnix.Load()) }
	now := func() time.Time { return cur() }

	var created int32
	factory := func() StrategyWithInfo {
		atomic.AddInt32(&created, 1)
		return trackingStrategy{}
	}

	hash := func(string) uint64 { return 0 }
	cfg := CleanupConfig{IdleTTL: 50 * time.Millisecond, CleanupEvery: 10 * time.Millisecond}

	kl := NewShardedKeyedLimiterWithInfoAndCleanup(factory, 2, hash, cfg, now)

	// Create key
	if allowed, _ := kl.Allow("user-1", cur()); !allowed {
		t.Fatalf("expected allowed")
	}
	if got := atomic.LoadInt32(&created); got != 1 {
		t.Fatalf("expected 1 strategy created, got %d", got)
	}

	// Close twice to ensure idempotence.
	kl.Close()
	kl.Close()

	// Advance beyond TTL and wait long enough that cleanup would have run.
	currentUnix.Add((200 * time.Millisecond).Nanoseconds())
	time.Sleep(50 * time.Millisecond)

	// If cleanup stopped, entry remains and factory is not called again.
	if allowed, _ := kl.Allow("user-1", cur()); !allowed {
		t.Fatalf("expected allowed")
	}
	if got := atomic.LoadInt32(&created); got != 1 {
		t.Fatalf("expected no recreation after Close; created=%d", got)
	}
}
