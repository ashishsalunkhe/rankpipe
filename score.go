package rankpipe

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

// itemFailure applies the item error policy to a failure of candidate i.
// It returns a non-nil error only when the stage must fail. Cancellation is
// never an item-level event: if ctx is done, the context error is returned
// regardless of policy.
func itemFailure(ctx context.Context, pol ItemErrorPolicy, i int, err error, count *atomic.Int64) error {
	if cerr := ctx.Err(); cerr != nil {
		return withCause(cerr, err)
	}
	if pol.kind == failStage {
		return &itemError{index: i, err: err}
	}
	count.Add(1)
	return nil
}

// withCause returns the context error, keeping the callback's own error as a
// secondary cause when it is a different one, so errors.Is sees both.
func withCause(cerr, err error) error {
	if errors.Is(err, cerr) {
		return cerr
	}
	return fmt.Errorf("%w (cause: %w)", cerr, err)
}

// compact removes candidates flagged in drop, preserving order, in place.
func compact[T any](in []Candidate[T], drop []bool) []Candidate[T] {
	out := in[:0]
	for i := range in {
		if !drop[i] {
			out = append(out, in[i])
		}
	}
	return out
}

func (s *stage[Req, T]) runFilter(ctx context.Context, req Req, in []Candidate[T]) ([]Candidate[T], stats, error) {
	n := len(in)
	if n == 0 {
		return in, stats{}, nil
	}
	pol := s.cfg.itemPolicy
	var itemErrs atomic.Int64

	if s.cfg.concurrency <= 1 {
		// Sequential path compacts in place with zero allocations.
		out := in[:0]
		for i := 0; i < n; i++ {
			if i&ctxCheckMask == 0 {
				if err := ctx.Err(); err != nil {
					return nil, stats{}, err
				}
			}
			keep, err := s.filter(ctx, req, in[i])
			if err != nil {
				if ferr := itemFailure(ctx, pol, i, err, &itemErrs); ferr != nil {
					return nil, stats{}, ferr
				}
				keep = pol.kind == keepItem
			}
			if keep {
				out = append(out, in[i])
			}
		}
		return out, stats{itemErrors: int(itemErrs.Load())}, nil
	}

	// Parallel path: decisions are recorded by index, then compacted
	// sequentially, so the outcome is independent of scheduling.
	drop := make([]bool, n)
	err := forEach(ctx, n, s.cfg.concurrency, func(i int) error {
		keep, err := s.filter(ctx, req, in[i])
		if err != nil {
			if ferr := itemFailure(ctx, pol, i, err, &itemErrs); ferr != nil {
				return ferr
			}
			keep = pol.kind == keepItem
		}
		drop[i] = !keep
		return nil
	})
	if err != nil {
		return nil, stats{}, err
	}
	return compact(in, drop), stats{itemErrors: int(itemErrs.Load())}, nil
}

func (s *stage[Req, T]) runScoreEach(ctx context.Context, req Req, in []Candidate[T]) ([]Candidate[T], stats, error) {
	n := len(in)
	if n == 0 {
		return in, stats{}, nil
	}
	pol := s.cfg.itemPolicy
	var itemErrs atomic.Int64
	var drop []bool
	if pol.kind == dropItem {
		drop = make([]bool, n)
	}

	err := forEach(ctx, n, s.cfg.concurrency, func(i int) error {
		sc, err := s.score(ctx, req, in[i])
		if err == nil && sc != sc {
			err = ErrNaNScore
		}
		if err != nil {
			if ferr := itemFailure(ctx, pol, i, err, &itemErrs); ferr != nil {
				return ferr
			}
			switch pol.kind {
			case dropItem:
				drop[i] = true
			case assignScore:
				in[i].Score = pol.score
			}
			// keepItem: previous score stays.
			return nil
		}
		in[i].Score = sc
		return nil
	})
	if err != nil {
		return nil, stats{}, err
	}
	st := stats{itemErrors: int(itemErrs.Load())}
	if drop != nil && st.itemErrors > 0 {
		in = compact(in, drop)
	}
	return in, st, nil
}

func (s *stage[Req, T]) runScoreBatch(ctx context.Context, req Req, in []Candidate[T]) ([]Candidate[T], stats, error) {
	n := len(in)
	if n == 0 {
		return in, stats{}, nil
	}
	pol := s.cfg.itemPolicy
	var itemErrs atomic.Int64
	var drop []bool
	if pol.kind == dropItem {
		drop = make([]bool, n)
	}

	size := s.cfg.maxBatch
	if size <= 0 || size > n {
		size = n
	}
	batches := (n + size - 1) / size

	scoreBatch := func(b int) error {
		lo := b * size
		hi := min(lo+size, n)
		// Clamp capacity so an accidental append in the scorer cannot clobber
		// the neighbouring batch.
		scores, err := s.batch(ctx, req, in[lo:hi:hi])
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return withCause(cerr, err)
			}
			if batches == 1 {
				return err
			}
			return fmt.Errorf("sub-batch %d [%d,%d): %w", b, lo, hi, err)
		}
		if len(scores) != hi-lo {
			return fmt.Errorf("%w: sub-batch %d [%d,%d): got %d scores for %d candidates",
				ErrBatchSizeMismatch, b, lo, hi, len(scores), hi-lo)
		}
		for j, sc := range scores {
			i := lo + j
			if sc != sc {
				if ferr := itemFailure(ctx, pol, i, ErrNaNScore, &itemErrs); ferr != nil {
					return ferr
				}
				switch pol.kind {
				case dropItem:
					drop[i] = true
				case assignScore:
					in[i].Score = pol.score
				}
				continue
			}
			in[i].Score = sc
		}
		return nil
	}

	var err error
	if batches == 1 {
		err = scoreBatch(0)
	} else {
		err = forEach(ctx, batches, s.cfg.concurrency, scoreBatch)
	}
	if err != nil {
		return nil, stats{}, err
	}
	st := stats{itemErrors: int(itemErrs.Load())}
	if drop != nil && st.itemErrors > 0 {
		in = compact(in, drop)
	}
	return in, st, nil
}
