package limiter_test

import (
	"testing"
	"time"

	. "example.com/rate-limiter/limiter"
)

func TestFixedWindowStrategyWithInfo_AllowsAndReportsRemainingAndReset(t *testing.T) {
	t.Parallel()

	s := NewFixedWindowStrategy(3, time.Minute)

	base := time.Date(2025, time.December, 9, 22, 0, 0, 0, time.UTC)
	resetAt := base.Add(time.Minute)

	allowed1, info1 := s.Allow(base)
	if !allowed1 {
		t.Fatalf("expected first allowed")
	}
	if info1.Limit != 3 || info1.Remaining != 2 || !info1.ResetAt.Equal(resetAt) {
		t.Fatalf("unexpected info1: %+v (expected Limit=3 Remaining=2 ResetAt=%v)", info1, resetAt)
	}

	allowed2, info2 := s.Allow(base.Add(10 * time.Second))
	if !allowed2 {
		t.Fatalf("expected second allowed")
	}
	if info2.Limit != 3 || info2.Remaining != 1 || !info2.ResetAt.Equal(resetAt) {
		t.Fatalf("unexpected info2: %+v", info2)
	}

	allowed3, info3 := s.Allow(base.Add(20 * time.Second))
	if !allowed3 {
		t.Fatalf("expected third allowed")
	}
	if info3.Limit != 3 || info3.Remaining != 0 || !info3.ResetAt.Equal(resetAt) {
		t.Fatalf("unexpected info3: %+v", info3)
	}

	allowed4, info4 := s.Allow(base.Add(30 * time.Second))
	if allowed4 {
		t.Fatalf("expected fourth denied")
	}
	if info4.Limit != 3 || info4.Remaining != 0 || !info4.ResetAt.Equal(resetAt) {
		t.Fatalf("unexpected info4: %+v", info4)
	}
}

func TestFixedWindowStrategyWithInfo_ResetsAfterWindowAndUpdatesResetAt(t *testing.T) {
	t.Parallel()

	s := NewFixedWindowStrategy(2, time.Minute)

	base := time.Date(2025, time.December, 9, 22, 10, 0, 0, time.UTC)
	resetAt1 := base.Add(time.Minute)

	// Use up window 1
	if allowed, _ := s.Allow(base); !allowed {
		t.Fatalf("expected allowed")
	}
	if allowed, _ := s.Allow(base.Add(10 * time.Second)); !allowed {
		t.Fatalf("expected allowed")
	}
	if allowed, _ := s.Allow(base.Add(20 * time.Second)); allowed {
		t.Fatalf("expected denied after limit reached")
	}

	// After window passes: should reset
	after := base.Add(time.Minute + time.Nanosecond)
	resetAt2 := after.Add(time.Minute)

	allowed, info := s.Allow(after)
	if !allowed {
		t.Fatalf("expected allowed after reset")
	}
	if info.Limit != 2 || info.Remaining != 1 || !info.ResetAt.Equal(resetAt2) {
		t.Fatalf("unexpected info after reset: %+v (expected Remaining=1 ResetAt=%v)", info, resetAt2)
	}

	// Ensure old resetAt is not reused
	if info.ResetAt.Equal(resetAt1) {
		t.Fatalf("expected resetAt to update after window reset")
	}
}

func TestTokenBucketStrategyWithInfo_AllowsAndReportsRemaining(t *testing.T) {
	t.Parallel()

	s := NewTokenBucketStrategy(2, 10*time.Second)

	base := time.Date(2025, time.December, 9, 22, 20, 0, 0, time.UTC)

	allowed1, info1 := s.Allow(base)
	if !allowed1 {
		t.Fatalf("expected first allowed")
	}
	if info1.Limit != 2 || info1.Remaining != 1 {
		t.Fatalf("unexpected info1: %+v", info1)
	}
	if !info1.ResetAt.Equal(base) {
		t.Fatalf("expected resetAt==at when tokens remain, got %v", info1.ResetAt)
	}

	allowed2, info2 := s.Allow(base)
	if !allowed2 {
		t.Fatalf("expected second allowed")
	}
	if info2.Limit != 2 || info2.Remaining != 0 {
		t.Fatalf("unexpected info2: %+v", info2)
	}

	// Now empty: denied and reset should be in the future (next refill time)
	deniedAt := base
	allowed3, info3 := s.Allow(deniedAt)
	if allowed3 {
		t.Fatalf("expected denied when empty")
	}
	if info3.Limit != 2 || info3.Remaining != 0 {
		t.Fatalf("unexpected info3: %+v", info3)
	}
	if !info3.ResetAt.After(deniedAt) {
		t.Fatalf("expected resetAt after deniedAt, got %v", info3.ResetAt)
	}
}

func TestTokenBucketStrategyWithInfo_RefillsAndUpdatesRemaining(t *testing.T) {
	t.Parallel()

	s := NewTokenBucketStrategy(1, 10*time.Second)

	base := time.Date(2025, time.December, 9, 22, 30, 0, 0, time.UTC)

	// Drain the single token
	if allowed, _ := s.Allow(base); !allowed {
		t.Fatalf("expected first allowed")
	}

	// Denied before refill
	if allowed, info := s.Allow(base.Add(9 * time.Second)); allowed {
		t.Fatalf("expected denied before refill")
	} else {
		if info.Remaining != 0 {
			t.Fatalf("expected remaining 0, got %d", info.Remaining)
		}
		if !info.ResetAt.After(base.Add(9 * time.Second)) {
			t.Fatalf("expected resetAt in the future")
		}
	}

	// Allowed at refill boundary
	allowed, info := s.Allow(base.Add(10 * time.Second))
	if !allowed {
		t.Fatalf("expected allowed at refill boundary")
	}
	if info.Limit != 1 || info.Remaining != 0 {
		t.Fatalf("unexpected info at refill: %+v", info)
	}
}
