package rankpipe

import (
	"context"
	"sync"
	"sync/atomic"
)

// ctxCheckMask controls how often the sequential loop polls the context:
// every 256 items. context.Err takes a mutex, which is measurable against a
// scorer that costs tens of nanoseconds; 256 items of any realistic callback
// is still far below any deadline granularity that matters.
const ctxCheckMask = 255

// forEach calls fn(i) for every i in [0, n) using at most `workers`
// goroutines, the caller's included. fn must be safe to call concurrently and
// must write its results by index so the outcome is independent of scheduling.
//
// fn returns an error only for stage-fatal conditions; tolerated item errors
// are handled inside fn. The first error stops the remaining work: a
// stage-scoped context is cancelled, every worker returns at its next chunk
// boundary, and forEach waits for all of them before returning. No goroutine
// outlives the call.
//
// If the context ends before all items were processed, forEach returns the
// context error even if no fn call failed, so partial results are never
// mistaken for complete ones.
func forEach(ctx context.Context, n, workers int, fn func(i int) error) error {
	if n == 0 {
		return nil
	}
	if workers > n {
		workers = n
	}
	if workers <= 1 {
		for i := 0; i < n; i++ {
			if i&ctxCheckMask == 0 {
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if err := fn(i); err != nil {
				return err
			}
		}
		return nil
	}

	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	chunk := chunkSize(n, workers)
	var (
		next     atomic.Int64
		firstErr error
		once     sync.Once
		wg       sync.WaitGroup
	)
	work := func() {
		for {
			if ctx.Err() != nil {
				return
			}
			start := int(next.Add(int64(chunk))) - chunk
			if start >= n {
				return
			}
			end := min(start+chunk, n)
			for i := start; i < end; i++ {
				if err := fn(i); err != nil {
					once.Do(func() {
						firstErr = err
						cancel()
					})
					return
				}
			}
		}
	}
	wg.Add(workers - 1)
	for w := 1; w < workers; w++ {
		go func() {
			defer wg.Done()
			work()
		}()
	}
	work()
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}
	// Workers may have stopped early because the parent context ended.
	return parent.Err()
}

// chunkSize picks how many indices a worker claims per atomic operation.
// Small inputs use chunk 1 so that one slow item (an RPC straggler) never
// leaves other workers idle; large inputs amortise the atomic across a chunk
// so that 100k cheap items do not pay 100k contended atomics.
func chunkSize(n, workers int) int {
	if n <= 4096 {
		return 1
	}
	c := n / (workers * 64)
	if c < 1 {
		return 1
	}
	if c > 64 {
		return 64
	}
	return c
}
