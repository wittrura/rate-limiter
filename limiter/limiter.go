package limiter

import "time"

type Limiter struct {
	maxRequests int
	window      time.Duration

	count int

	windowStart time.Time
}

func NewLimiter(maxRequests int, window time.Duration) *Limiter {
	return &Limiter{
		maxRequests: maxRequests,
		window:      window,
	}
}

func (l *Limiter) Allow(at time.Time) bool {
	if l.windowStart.IsZero() {
		l.windowStart = at
		l.count = 1
		return true
	}

	if at.Before(l.windowStart.Add(l.window)) {
		if l.count < l.maxRequests {
			l.count++
			return true
		}
	}

	return false
}
