package limiter

import (
	"sync"
	"time"
)

type Limiter struct {
	maxRequests int
	window      time.Duration
	count       int

	windowStart time.Time

	mu sync.Mutex
}

func NewLimiter(maxRequests int, window time.Duration) *Limiter {
	if maxRequests < 1 {
		panic("cannot handle maxRequests less than 1")
	}

	if window <= time.Duration(0) {
		panic("cannot handle zero or negative window")
	}

	return &Limiter{
		maxRequests: maxRequests,
		window:      window,
	}
}

func (l *Limiter) Allow(at time.Time) bool {
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

type RateLimiter struct {
	maxRequests int
	window      time.Duration

	limiters map[string]*Limiter

	mu sync.Mutex
}

func NewRateLimiter(maxRequests int, window time.Duration) *RateLimiter {
	if maxRequests < 1 {
		panic("cannot handle maxRequests less than 1")
	}

	if window <= time.Duration(0) {
		panic("cannot handle zero or negative window")
	}

	return &RateLimiter{
		maxRequests: maxRequests,
		window:      window,
		limiters:    make(map[string]*Limiter),
	}
}

func (rl *RateLimiter) Allow(key string, at time.Time) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	limiter, ok := rl.limiters[key]
	if !ok {
		limiter = NewLimiter(rl.maxRequests, rl.window)
		rl.limiters[key] = limiter
	}
	return limiter.Allow(at)
}

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

type KeyedLimiterWithInfo struct {
	strategies map[string]StrategyWithInfo
	factory    func() StrategyWithInfo

	mu sync.Mutex
}

func NewKeyedLimiterWithInfo(factory func() StrategyWithInfo) *KeyedLimiterWithInfo {
	if factory == nil {
		panic("limiter requires non-nil strategy")
	}

	return &KeyedLimiterWithInfo{
		factory:    factory,
		strategies: make(map[string]StrategyWithInfo),
	}
}

func (kl *KeyedLimiterWithInfo) Allow(key string, at time.Time) (bool, Info) {
	kl.mu.Lock()
	defer kl.mu.Unlock()

	var strategy StrategyWithInfo
	strategy, ok := kl.strategies[key]
	if !ok {
		strategy = kl.factory()
		kl.strategies[key] = strategy
	}

	return strategy.Allow(at)
}
