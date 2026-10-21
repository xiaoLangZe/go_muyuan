package download

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"time"
)

// httpStatusError carries a non-success HTTP status. 5xx, 408 and 429 are
// retryable; every other 4xx is permanent.
type httpStatusError struct {
	code int
	url  string
}

func (e *httpStatusError) Error() string {
	return fmt.Sprintf("download: unexpected http status %d from %s", e.code, e.url)
}

func (e *httpStatusError) retryable() bool {
	return e.code >= 500 || e.code == http.StatusRequestTimeout ||
		e.code == http.StatusTooManyRequests
}

func isPermanent(err error) bool {
	if err == nil {
		return false
	}
	var se *httpStatusError
	if errors.As(err, &se) {
		return !se.retryable()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if errors.Is(err, errRangeChanged) {
		return true
	}
	return false
}

func sleepBackoff(ctx context.Context, attempt int, base time.Duration) error {
	if base <= 0 {
		base = 500 * time.Millisecond
	}
	d := base << uint(attempt)
	const maxDelay = 30 * time.Second
	if d > maxDelay || d <= 0 {
		d = maxDelay
	}
	// +/-25% jitter so parallel workers do not retry in lockstep.
	jitter := 0.75 + rand.Float64()*0.5
	d = time.Duration(float64(d) * jitter)
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// retryDo runs fn up to attempts times, backing off between failures. Permanent
// errors are returned immediately.
func retryDo(ctx context.Context, attempts int, base time.Duration, fn func() error) error {
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := fn()
		if err == nil {
			return nil
		}
		last = err
		if isPermanent(err) || i == attempts-1 {
			return err
		}
		if err := sleepBackoff(ctx, i, base); err != nil {
			return err
		}
	}
	return last
}
