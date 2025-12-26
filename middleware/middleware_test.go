package middleware_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"example.com/rate-limiter/limiter"
	. "example.com/rate-limiter/middleware"
)

type fixedAllowanceStrategy struct {
	remaining int64
}

func newFixedAllowanceStrategy(allows int) *fixedAllowanceStrategy {
	return &fixedAllowanceStrategy{remaining: int64(allows)}
}

func (s *fixedAllowanceStrategy) Allow(_ time.Time) bool {
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

func TestNewRateLimit_PanicsOnNilKeyedLimiter(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when keyed limiter is nil")
		}
	}()

	_ = NewRateLimit(nil, func(*http.Request) string { return "k" })
}

func TestNewRateLimit_PanicsOnNilKeyFunc(t *testing.T) {
	t.Parallel()

	kl := limiter.NewKeyedLimiter(func() limiter.Strategy { return newFixedAllowanceStrategy(1) })

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when keyFunc is nil")
		}
	}()

	_ = NewRateLimit(kl, nil)
}

func TestRateLimitMiddleware_AllowsRequestAndCallsNext(t *testing.T) {
	t.Parallel()

	kl := limiter.NewKeyedLimiter(func() limiter.Strategy { return newFixedAllowanceStrategy(1) })

	var nextCalls int32
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nextCalls, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	keyFunc := func(r *http.Request) string { return "user-1" }

	handler := NewRateLimit(kl, keyFunc)(next)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	rr := httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	if atomic.LoadInt32(&nextCalls) != 1 {
		t.Fatalf("expected next handler to be called once, got %d", atomic.LoadInt32(&nextCalls))
	}
	body, _ := io.ReadAll(rr.Body)
	if string(body) != "ok" {
		t.Fatalf("expected body %q, got %q", "ok", string(body))
	}
}

func TestRateLimitMiddleware_BlocksWhenNotAllowed_AndDoesNotCallNext(t *testing.T) {
	t.Parallel()

	// Strategy allows exactly 1 request per key.
	kl := limiter.NewKeyedLimiter(func() limiter.Strategy { return newFixedAllowanceStrategy(1) })

	var nextCalls int32
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nextCalls, 1)
		w.WriteHeader(http.StatusOK)
	})

	keyFunc := func(r *http.Request) string { return "user-1" }
	handler := NewRateLimit(kl, keyFunc)(next)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)

	// First request allowed
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected first request 200 OK, got %d", rr1.Code)
	}

	// Second request denied
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req)
	if rr2.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second request 429 Too Many Requests, got %d", rr2.Code)
	}

	if atomic.LoadInt32(&nextCalls) != 1 {
		t.Fatalf("expected next handler to be called exactly once total, got %d", atomic.LoadInt32(&nextCalls))
	}
}

func TestRateLimitMiddleware_DifferentKeysAreIndependent(t *testing.T) {
	t.Parallel()

	// Each key gets a strategy that allows exactly 1 request.
	kl := limiter.NewKeyedLimiter(func() limiter.Strategy { return newFixedAllowanceStrategy(1) })

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Use header-driven keying to simulate multiple callers.
	keyFunc := func(r *http.Request) string { return r.Header.Get("X-User") }
	handler := NewRateLimit(kl, keyFunc)(next)

	reqA := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	reqA.Header.Set("X-User", "A")

	reqB := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	reqB.Header.Set("X-User", "B")

	// First call for each key allowed.
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, reqA)
	if rr1.Code != http.StatusOK {
		t.Fatalf("expected key A first request 200 OK, got %d", rr1.Code)
	}

	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, reqB)
	if rr2.Code != http.StatusOK {
		t.Fatalf("expected key B first request 200 OK, got %d", rr2.Code)
	}

	// Second call for each key denied (independently).
	rr3 := httptest.NewRecorder()
	handler.ServeHTTP(rr3, reqA)
	if rr3.Code != http.StatusTooManyRequests {
		t.Fatalf("expected key A second request 429, got %d", rr3.Code)
	}

	rr4 := httptest.NewRecorder()
	handler.ServeHTTP(rr4, reqB)
	if rr4.Code != http.StatusTooManyRequests {
		t.Fatalf("expected key B second request 429, got %d", rr4.Code)
	}
}

func TestRateLimitMiddleware_KeyFuncIsUsed(t *testing.T) {
	t.Parallel()

	kl := limiter.NewKeyedLimiter(func() limiter.Strategy { return newFixedAllowanceStrategy(1) })

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	var keyFuncCalls int32
	keyFunc := func(r *http.Request) string {
		atomic.AddInt32(&keyFuncCalls, 1)
		return "user-1"
	}

	handler := NewRateLimit(kl, keyFunc)(next)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if atomic.LoadInt32(&keyFuncCalls) != 1 {
		t.Fatalf("expected keyFunc to be called once, got %d", atomic.LoadInt32(&keyFuncCalls))
	}
}

type fixedInfoStrategy struct {
	allowed bool
	info    limiter.Info
}

func (s fixedInfoStrategy) Allow(_ time.Time) (bool, limiter.Info) {
	return s.allowed, s.info
}

func TestNewRateLimitWithInfo_PanicsOnNilKeyedLimiter(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when keyed limiter is nil")
		}
	}()

	_ = NewRateLimitWithInfo(nil, func(*http.Request) string { return "k" }, func() time.Time { return time.Now() })
}

func TestNewRateLimitWithInfo_PanicsOnNilKeyFunc(t *testing.T) {
	t.Parallel()

	kl := limiter.NewKeyedLimiterWithInfo(func() limiter.StrategyWithInfo {
		return fixedInfoStrategy{allowed: true, info: limiter.Info{Limit: 1, Remaining: 0, ResetAt: time.Unix(0, 0)}}
	})

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic when keyFunc is nil")
		}
	}()

	_ = NewRateLimitWithInfo(kl, nil, func() time.Time { return time.Now() })
}

func TestNewRateLimitWithInfo_DefaultsNow(t *testing.T) {
	t.Parallel()

	kl := limiter.NewKeyedLimiterWithInfo(func() limiter.StrategyWithInfo {
		return fixedInfoStrategy{allowed: true, info: limiter.Info{Limit: 1, Remaining: 0, ResetAt: time.Unix(0, 0)}}
	})

	_ = NewRateLimitWithInfo(kl, func(*http.Request) string { return "k" }, nil)
}

func TestRateLimitWithInfo_SetsHeadersOnAllowed_AndCallsNext(t *testing.T) {
	t.Parallel()

	resetAt := time.Date(2025, time.December, 9, 21, 0, 0, 0, time.UTC)

	kl := limiter.NewKeyedLimiterWithInfo(func() limiter.StrategyWithInfo {
		return fixedInfoStrategy{
			allowed: true,
			info: limiter.Info{
				Limit:     10,
				Remaining: 9,
				ResetAt:   resetAt,
			},
		}
	})

	now := func() time.Time { return time.Date(2025, time.December, 9, 20, 59, 0, 0, time.UTC) }

	var nextCalls int32
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nextCalls, 1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	keyFunc := func(r *http.Request) string { return "user-1" }
	handler := NewRateLimitWithInfo(kl, keyFunc, now)(next)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}
	if atomic.LoadInt32(&nextCalls) != 1 {
		t.Fatalf("expected next handler to be called once, got %d", atomic.LoadInt32(&nextCalls))
	}

	if got := rr.Header().Get("X-RateLimit-Limit"); got != "10" {
		t.Fatalf("expected X-RateLimit-Limit=10, got %q", got)
	}
	if got := rr.Header().Get("X-RateLimit-Remaining"); got != "9" {
		t.Fatalf("expected X-RateLimit-Remaining=9, got %q", got)
	}
	if got := rr.Header().Get("X-RateLimit-Reset"); got != strconv.FormatInt(resetAt.Unix(), 10) {
		t.Fatalf("expected X-RateLimit-Reset=%d, got %q", resetAt.Unix(), got)
	}

	if got := rr.Header().Get("Retry-After"); got != "" {
		t.Fatalf("expected Retry-After to be empty on allowed response, got %q", got)
	}

	body, _ := io.ReadAll(rr.Body)
	if string(body) != "ok" {
		t.Fatalf("expected body %q, got %q", "ok", string(body))
	}
}

func TestRateLimitWithInfo_OnDenied_Returns429_DoesNotCallNext_SetsRetryAfter(t *testing.T) {
	t.Parallel()

	nowTime := time.Date(2025, time.December, 9, 21, 0, 0, 0, time.UTC)
	resetAt := nowTime.Add(5 * time.Second)

	kl := limiter.NewKeyedLimiterWithInfo(func() limiter.StrategyWithInfo {
		return fixedInfoStrategy{
			allowed: false,
			info: limiter.Info{
				Limit:     10,
				Remaining: 0,
				ResetAt:   resetAt,
			},
		}
	})

	now := func() time.Time { return nowTime }

	var nextCalls int32
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&nextCalls, 1)
		w.WriteHeader(http.StatusOK)
	})

	keyFunc := func(r *http.Request) string { return "user-1" }
	handler := NewRateLimitWithInfo(kl, keyFunc, now)(next)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rr.Code)
	}
	if atomic.LoadInt32(&nextCalls) != 0 {
		t.Fatalf("expected next handler not called, got %d", atomic.LoadInt32(&nextCalls))
	}

	// Always present:
	if got := rr.Header().Get("X-RateLimit-Limit"); got != "10" {
		t.Fatalf("expected X-RateLimit-Limit=10, got %q", got)
	}
	if got := rr.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Fatalf("expected X-RateLimit-Remaining=0, got %q", got)
	}
	if got := rr.Header().Get("X-RateLimit-Reset"); got != strconv.FormatInt(resetAt.Unix(), 10) {
		t.Fatalf("expected X-RateLimit-Reset=%d, got %q", resetAt.Unix(), got)
	}

	// Retry-After is seconds until reset (rounded down) and clamped >= 0
	if got := rr.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("expected Retry-After=5, got %q", got)
	}
}

func TestRateLimitWithInfo_RetryAfterClampedToZeroIfResetInPast(t *testing.T) {
	t.Parallel()

	nowTime := time.Date(2025, time.December, 9, 21, 0, 0, 0, time.UTC)
	resetAt := nowTime.Add(-1 * time.Second)

	kl := limiter.NewKeyedLimiterWithInfo(func() limiter.StrategyWithInfo {
		return fixedInfoStrategy{
			allowed: false,
			info: limiter.Info{
				Limit:     10,
				Remaining: 0,
				ResetAt:   resetAt,
			},
		}
	})

	now := func() time.Time { return nowTime }

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	keyFunc := func(r *http.Request) string { return "user-1" }
	handler := NewRateLimitWithInfo(kl, keyFunc, now)(next)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", rr.Code)
	}

	if got := rr.Header().Get("Retry-After"); got != "0" {
		t.Fatalf("expected Retry-After=0 when reset is in the past, got %q", got)
	}
}

func TestRateLimitWithInfo_KeyFuncIsUsed(t *testing.T) {
	t.Parallel()

	var keyFuncCalls int32
	keyFunc := func(r *http.Request) string {
		atomic.AddInt32(&keyFuncCalls, 1)
		return "user-1"
	}

	kl := limiter.NewKeyedLimiterWithInfo(func() limiter.StrategyWithInfo {
		return fixedInfoStrategy{
			allowed: true,
			info: limiter.Info{
				Limit:     1,
				Remaining: 0,
				ResetAt:   time.Unix(0, 0),
			},
		}
	})

	now := func() time.Time { return time.Unix(0, 0) }

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	handler := NewRateLimitWithInfo(kl, keyFunc, now)(next)

	req := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if atomic.LoadInt32(&keyFuncCalls) != 1 {
		t.Fatalf("expected keyFunc called once, got %d", atomic.LoadInt32(&keyFuncCalls))
	}
}
