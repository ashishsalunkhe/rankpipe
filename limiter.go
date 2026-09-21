package rankpipe

import "context"

// Limiter bounds how many stage invocations may run concurrently across all
// Run calls that share it. It exists because a stage with WithConcurrency(16)
// invoked by 10 000 concurrent requests would otherwise want 160 000
// goroutines; with a Limiter of 64 the bound is 64×16.
//
// It is intentionally minimal — a counting semaphore with a context-aware
// acquire. Admission control beyond that (priorities, adaptive limits) belongs
// to the application.
type Limiter struct {
	sem chan struct{}
}

// NewLimiter returns a Limiter allowing n concurrent holders. n must be >= 1.
func NewLimiter(n int) *Limiter {
	if n < 1 {
		panic("rankpipe: NewLimiter requires n >= 1")
	}
	return &Limiter{sem: make(chan struct{}, n)}
}

// Acquire blocks until a permit is available or ctx is done. It never waits
// when ctx is already done, so work that cannot start before the deadline
// fails instead of queueing.
func (l *Limiter) Acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case l.sem <- struct{}{}:
		return nil
	default:
	}
	select {
	case l.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns a permit. It must be called exactly once per successful Acquire.
func (l *Limiter) Release() {
	select {
	case <-l.sem:
	default:
		panic("rankpipe: Limiter.Release without Acquire")
	}
}
