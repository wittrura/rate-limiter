package middleware_test

import (
	"io"
	"net/http"
	"net/http/httptest"
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
