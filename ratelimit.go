package download

import (
	"context"
	"math"
	"sync"
	"time"
)

// limiter is a small shared token bucket. The package deliberately avoids an
// external dependency: all it needs is a byte-level WaitN with context
// cancellation.
type limiter struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64
	tokens float64
	last   time.Time
}

func newLimiter(bytesPerSecond int64) *limiter {
	burst := math.Max(float64(bytesPerSecond), 64*1024)
	return &limiter{
		rate:   float64(bytesPerSecond),
		burst:  burst,
		tokens: burst,
		last:   time.Now(),
	}
}

func (l *limiter) waitN(ctx context.Context, n int) error {
	if l == nil || l.rate <= 0 || n <= 0 {
		return nil
	}
	want := float64(n)
	if want > l.burst {
		want = l.burst
	}
	for {
		l.mu.Lock()
		now := time.Now()
		if elapsed := now.Sub(l.last).Seconds(); elapsed > 0 {
			l.tokens = math.Min(l.burst, l.tokens+elapsed*l.rate)
			l.last = now
		}
		if l.tokens >= want {
			l.tokens -= want
			l.mu.Unlock()
			return nil
		}
		wait := (want - l.tokens) / l.rate
		l.mu.Unlock()

		// Cap the sleep so the loop stays responsive to cancellation and so
		// several waiters share the bucket fairly enough.
		delay := time.Duration(wait * float64(time.Second))
		if delay < time.Millisecond {
			delay = time.Millisecond
		} else if delay > 50*time.Millisecond {
			delay = 50 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
