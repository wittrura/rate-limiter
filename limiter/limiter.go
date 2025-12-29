package limiter

import (
	"context"
	"sync"
	"time"
)

// single-key algorithm
type FixedWindow struct {
	maxRequests int
	window      time.Duration
	count       int

	windowStart time.Time

	mu sync.Mutex
}

func NewFixedWindow(maxRequests int, window time.Duration) *FixedWindow {
	if maxRequests < 1 {
		panic("cannot handle maxRequests less than 1")
	}

	if window <= time.Duration(0) {
		panic("cannot handle zero or negative window")
	}

	return &FixedWindow{
		maxRequests: maxRequests,
		window:      window,
	}
}

func (l *FixedWindow) Allow(at time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.windowStart.IsZero() {
		l.windowStart = at
		l.count = 1
		return true
	}

	if at.After(l.windowStart.Add(l.window)) {
		l.windowStart = at
		l.count = 1
		return true
	}

	if l.count < l.maxRequests {
		l.count++
		return true
	}

	return false
}

// keyed dispatcher
type FixedWindowLimiter struct {
	maxRequests int
	window      time.Duration

	limiters map[string]*FixedWindow

	mu sync.Mutex
}

func NewFixedWindowLimiter(maxRequests int, window time.Duration) *FixedWindowLimiter {
	if maxRequests < 1 {
		panic("cannot handle maxRequests less than 1")
	}

	if window <= time.Duration(0) {
		panic("cannot handle zero or negative window")
	}

	return &FixedWindowLimiter{
		maxRequests: maxRequests,
		window:      window,
		limiters:    make(map[string]*FixedWindow),
	}
}

func (rl *FixedWindowLimiter) Allow(key string, at time.Time) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	limiter, ok := rl.limiters[key]
	if !ok {
		limiter = NewFixedWindow(rl.maxRequests, rl.window)
		rl.limiters[key] = limiter
	}
	return limiter.Allow(at)
}

// single-key algorithm
type TokenBucket struct {
	tokens      int
	capacity    int
	refillEvery time.Duration

	lastRefill time.Time
}

func NewTokenBucket(maxTokens int, refillEvery time.Duration) *TokenBucket {
	if maxTokens < 1 {
		panic("cannot handle maxTokens less than 1")
	}

	if refillEvery <= time.Duration(0) {
		panic("cannot handle zero or negative refillEvery")
	}

	return &TokenBucket{
		tokens:      maxTokens,
		capacity:    maxTokens,
		refillEvery: refillEvery,
	}
}

func (tb *TokenBucket) Allow(at time.Time) bool {
	if tb.lastRefill.IsZero() {
		tb.lastRefill = at
	}

	var intervals int
	if at.After(tb.lastRefill) {
		sinceLastRefill := at.Sub(tb.lastRefill)
		intervals = int(sinceLastRefill / tb.refillEvery)
	}

	if intervals > 0 {
		// add one token for each interval but cap at capacity
		tb.tokens = min(tb.capacity, tb.tokens+intervals)

		advance := time.Duration(intervals) * tb.refillEvery
		tb.lastRefill = tb.lastRefill.Add(advance)
	}

	if tb.tokens > 0 {
		tb.tokens--
		return true
	}

	return false
}

type Strategy interface {
	Allow(at time.Time) bool
}

type StrategyFactory func() Strategy

// keyed dispatcher - default
type KeyedLimiter struct {
	strategies map[string]Strategy
	factory    StrategyFactory

	mu sync.Mutex
}

func NewKeyedLimiter(factory StrategyFactory) *KeyedLimiter {
	if factory == nil {
		panic("limiter requires non-nil strategy")
	}

	return &KeyedLimiter{
		factory:    factory,
		strategies: make(map[string]Strategy),
	}
}

func (kl *KeyedLimiter) Allow(key string, at time.Time) bool {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	var strategy Strategy
	strategy, ok := kl.strategies[key]
	if !ok {
		strategy = kl.factory()
		kl.strategies[key] = strategy
	}

	return strategy.Allow(at)
}

type Info struct {
	Limit     int
	Remaining int
	ResetAt   time.Time
}

type StrategyWithInfo interface {
	Allow(at time.Time) (allowed bool, info Info)
}

type entry struct {
	strategy   StrategyWithInfo
	lastAccess time.Time
}

// keyed dispatcher WITH INFO - default
type KeyedLimiterWithInfo struct {
	entries map[string]entry
	factory func() StrategyWithInfo

	cleanupTicker *time.Ticker
	cleanupCancel context.CancelFunc
	isClosed      bool

	mu sync.Mutex
}

func NewKeyedLimiterWithInfo(factory func() StrategyWithInfo) *KeyedLimiterWithInfo {
	if factory == nil {
		panic("limiter requires non-nil strategy")
	}

	return &KeyedLimiterWithInfo{
		factory: factory,
		entries: make(map[string]entry),
	}
}

type CleanupConfig struct {
	IdleTTL      time.Duration
	CleanupEvery time.Duration
}

func NewKeyedLimiterWithInfoAndCleanup(factory func() StrategyWithInfo, cfg CleanupConfig, now func() time.Time) *KeyedLimiterWithInfo {
	if factory == nil {
		panic("limiter requires non-nil strategy")
	}

	if cfg.IdleTTL <= 0 {
		panic("cleanup requires IdleTTL > 0")
	}
	if cfg.CleanupEvery <= 0 {
		panic("cleanup requires CleanupEvery > 0")
	}
	if now == nil {
		panic("cleanup requires non-nil now() parameter")
	}

	ctx, cancel := context.WithCancel(context.Background())
	ticker := time.NewTicker(cfg.CleanupEvery)

	limiter := &KeyedLimiterWithInfo{
		factory:       factory,
		entries:       make(map[string]entry),
		cleanupTicker: ticker,
		cleanupCancel: cancel,
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				limiter.mu.Lock()
				cutoff := now().Add(-cfg.IdleTTL)
				for key, entry := range limiter.entries {
					if entry.lastAccess.Before(cutoff) {
						delete(limiter.entries, key)
					}
				}
				limiter.mu.Unlock()
			}
		}
	}()

	return limiter
}

func (kl *KeyedLimiterWithInfo) Allow(key string, at time.Time) (bool, Info) {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	e, ok := kl.entries[key]
	if !ok {
		e = entry{strategy: kl.factory()}
	}
	e.lastAccess = at
	kl.entries[key] = e

	return e.strategy.Allow(at)
}

func (kl *KeyedLimiterWithInfo) Close() {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	if kl.isClosed {
		return
	}
	kl.cleanupCancel()
	kl.cleanupTicker.Stop()
	kl.isClosed = true
}

type FixedWindowStrategy struct {
	limiter *FixedWindow
}

func (f *FixedWindowStrategy) Allow(at time.Time) (allowed bool, info Info) {
	allowed = f.limiter.Allow(at)

	limit := f.limiter.maxRequests
	remaining := limit - f.limiter.count
	resetAt := f.limiter.windowStart.Add(f.limiter.window)

	info = Info{
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   resetAt,
	}

	return allowed, info
}

var _ StrategyWithInfo = (*FixedWindowStrategy)(nil)

func NewFixedWindowStrategy(maxRequests int, window time.Duration) StrategyWithInfo {
	return &FixedWindowStrategy{
		limiter: NewFixedWindow(maxRequests, window),
	}
}

type TokenBucketStrategy struct {
	limiter *TokenBucket
}

func (f *TokenBucketStrategy) Allow(at time.Time) (allowed bool, info Info) {
	allowed = f.limiter.Allow(at)

	limit := f.limiter.capacity
	remaining := f.limiter.tokens

	var resetAt time.Time
	if remaining > 0 {
		resetAt = at
	} else {
		resetAt = f.limiter.lastRefill.Add(f.limiter.refillEvery)
	}

	info = Info{
		Limit:     limit,
		Remaining: remaining,
		ResetAt:   resetAt,
	}

	return allowed, info
}

var _ StrategyWithInfo = (*TokenBucketStrategy)(nil)

func NewTokenBucketStrategy(maxTokens int, refillEvery time.Duration) StrategyWithInfo {
	return &TokenBucketStrategy{
		limiter: NewTokenBucket(maxTokens, refillEvery),
	}
}
