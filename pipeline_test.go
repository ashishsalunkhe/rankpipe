package rankpipe

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type req struct{ user string }

func items(n int) []Candidate[int] {
	out := make([]Candidate[int], n)
	for i := range out {
		out[i] = Candidate[int]{Item: i, Score: float64(i)}
	}
	return out
}

func ids(cs []Candidate[int]) []int { return Items(cs) }

func mustNew[Req, T any](t *testing.T, stages ...Stage[Req, T]) *Pipeline[Req, T] {
	t.Helper()
	p, err := New(stages...)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestFunnel(t *testing.T) {
	even := func(_ context.Context, _ req, c Candidate[int]) (bool, error) { return c.Item%2 == 0, nil }
	negate := func(_ context.Context, _ req, c Candidate[int]) (float64, error) { return -float64(c.Item), nil }
	double := func(_ context.Context, _ req, in []Candidate[int]) ([]float64, error) {
		out := make([]float64, len(in))
		for i, c := range in {
			out[i] = 2 * c.Score
		}
		return out, nil
	}
	reverse := func(_ context.Context, _ req, in []Candidate[int]) ([]Candidate[int], error) {
		slices.Reverse(in)
		return in, nil
	}
	p := mustNew(t,
		Filter("even", even),
		ScoreEach("negate", negate, WithConcurrency(4)),
		TopK[req, int]("top10", 10),
		ScoreBatch("double", double, WithMaxBatchSize(3)),
		TopK[req, int]("top4", 4),
		Rerank("reverse", reverse),
	)
	in := items(100)
	orig := slices.Clone(in)
	res, err := p.Run(context.Background(), req{}, in)
	if err != nil {
		t.Fatal(err)
	}
	// even: 0,2,...,98; negate: best are smallest; top10: 0..18; double; top4: 0,2,4,6; reverse.
	if got, want := ids(res.Candidates), []int{6, 4, 2, 0}; !slices.Equal(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
	if res.Candidates[0].Score != -12 {
		t.Fatalf("score not doubled: %v", res.Candidates[0].Score)
	}
	if res.Degraded {
		t.Fatal("unexpected degraded")
	}
	if !slices.Equal(in, orig) {
		t.Fatal("Run modified the caller's input slice")
	}
	counts := [][2]int{{100, 50}, {50, 50}, {50, 10}, {10, 10}, {10, 4}, {4, 4}}
	if len(res.Stages) != 6 {
		t.Fatalf("reports: %+v", res.Stages)
	}
	for i, r := range res.Stages {
		if r.In != counts[i][0] || r.Out != counts[i][1] || r.Status != StatusOK || r.Index != i {
			t.Fatalf("report %d: %+v", i, r)
		}
	}
}

func TestInputNotAliased(t *testing.T) {
	p := mustNew(t, Rerank("rev", func(_ context.Context, _ req, in []Candidate[int]) ([]Candidate[int], error) {
		slices.Reverse(in)
		return in, nil
	}))
	in := items(5)
	res, _ := p.Run(context.Background(), req{}, in)
	res.Candidates[0].Score = 999
	if in[0].Score == 999 || in[4].Score == 999 {
		t.Fatal("result aliases input")
	}
}

func TestEmptyAndNilInput(t *testing.T) {
	calls := 0
	p := mustNew(t,
		Filter("f", func(_ context.Context, _ req, _ Candidate[int]) (bool, error) { calls++; return true, nil }),
		ScoreEach("s", func(_ context.Context, _ req, _ Candidate[int]) (float64, error) { calls++; return 1, nil }),
		ScoreBatch("b", func(_ context.Context, _ req, _ []Candidate[int]) ([]float64, error) { calls++; return nil, nil }),
		TopK[req, int]("k", 3),
		Rerank("r", func(_ context.Context, _ req, in []Candidate[int]) ([]Candidate[int], error) { calls++; return in, nil }),
		Transform("t", func(_ context.Context, _ req, in []Candidate[int]) ([]Candidate[int], error) {
			calls += 100
			return append(in, Candidate[int]{Item: 7}), nil
		}),
	)
	for _, in := range [][]Candidate[int]{nil, {}} {
		calls = 0
		res, err := p.Run(context.Background(), req{}, in)
		if err != nil {
			t.Fatal(err)
		}
		if calls != 100 {
			t.Fatalf("only Transform should run on empty input; calls=%d", calls)
		}
		if len(res.Candidates) != 1 || res.Candidates[0].Item != 7 {
			t.Fatalf("got %v", res.Candidates)
		}
	}
}

func TestContinueWithInputSnapshotsInput(t *testing.T) {
	// Scorer writes half the scores then fails; the next stage must see the
	// original scores, not the half-written ones.
	var n atomic.Int64
	p := mustNew(t,
		ScoreEach("flaky", func(_ context.Context, _ req, c Candidate[int]) (float64, error) {
			if n.Add(1) > 3 {
				return 0, errors.New("boom")
			}
			return 1000, nil
		}, WithErrorPolicy(ContinueWithInput)),
		TopK[req, int]("k", 10),
	)
	res, err := p.Run(context.Background(), req{}, items(6))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Degraded {
		t.Fatal("expected degraded")
	}
	if got := ids(res.Candidates); !slices.Equal(got, []int{5, 4, 3, 2, 1, 0}) {
		t.Fatalf("got %v", got)
	}
	r := res.Stages[0]
	if r.Status != StatusDegraded || r.Err == nil || r.In != 6 || r.Out != 6 {
		t.Fatalf("report %+v", r)
	}
	var e *Error
	if !errors.As(r.Err, &e) || e.ItemIndex != 3 || e.Op != OpScore {
		t.Fatalf("report err %v", r.Err)
	}
}

func TestFallback(t *testing.T) {
	deep := ScoreEach("deep", func(_ context.Context, _ req, _ Candidate[int]) (float64, error) {
		return 0, errors.New("model down")
	}, WithFallback(ScoreEach("light", func(_ context.Context, _ req, c Candidate[int]) (float64, error) {
		return -float64(c.Item), nil
	})))
	p := mustNew(t, deep, TopK[req, int]("k", 2))
	res, err := p.Run(context.Background(), req{}, items(5))
	if err != nil {
		t.Fatal(err)
	}
	if got := ids(res.Candidates); !slices.Equal(got, []int{0, 1}) {
		t.Fatalf("got %v", got)
	}
	if !res.Degraded || len(res.Stages) != 3 {
		t.Fatalf("res %+v", res)
	}
	if res.Stages[0].Status != StatusFellBack || res.Stages[0].Name != "deep" {
		t.Fatalf("primary report %+v", res.Stages[0])
	}
	if !res.Stages[1].Fallback || res.Stages[1].Name != "light" || res.Stages[1].Index != 0 || res.Stages[1].Status != StatusOK {
		t.Fatalf("fallback report %+v", res.Stages[1])
	}
	if res.Stages[2].Name != "k" || res.Stages[2].Index != 1 {
		t.Fatalf("next report %+v", res.Stages[2])
	}
}

func TestFallbackFailureFailsPipeline(t *testing.T) {
	fail := func(_ context.Context, _ req, _ Candidate[int]) (float64, error) { return 0, errors.New("x") }
	p := mustNew(t, ScoreEach("a", fail, WithFallback(ScoreEach("b", fail))))
	res, err := p.Run(context.Background(), req{}, items(3))
	var e *Error
	if !errors.As(err, &e) || e.Stage != "b" {
		t.Fatalf("err %v", err)
	}
	if res.Candidates != nil || len(res.Stages) != 2 || res.Stages[1].Status != StatusFailed {
		t.Fatalf("res %+v", res)
	}
}

func TestItemPolicies(t *testing.T) {
	failOdd := func(_ context.Context, _ req, c Candidate[int]) (float64, error) {
		if c.Item%2 == 1 {
			return 0, fmt.Errorf("odd %d", c.Item)
		}
		return 100, nil
	}
	nanOdd := func(_ context.Context, _ req, c Candidate[int]) (float64, error) {
		if c.Item%2 == 1 {
			return math.NaN(), nil
		}
		return 100, nil
	}
	for _, conc := range []int{0, 4} {
		for name, fn := range map[string]ScoreFunc[req, int]{"err": failOdd, "nan": nanOdd} {
			t.Run(fmt.Sprintf("%s/conc=%d", name, conc), func(t *testing.T) {
				// FailStage
				p := mustNew(t, ScoreEach("s", fn, WithConcurrency(conc)))
				_, err := p.Run(context.Background(), req{}, items(6))
				var e *Error
				if !errors.As(err, &e) || e.ItemIndex < 0 || e.ItemIndex%2 != 1 {
					t.Fatalf("FailStage: %v", err)
				}
				if name == "nan" && !errors.Is(err, ErrNaNScore) {
					t.Fatalf("expected ErrNaNScore, got %v", err)
				}
				// DropItem
				p = mustNew(t, ScoreEach("s", fn, WithConcurrency(conc), WithItemErrorPolicy(DropItem)))
				res, err := p.Run(context.Background(), req{}, items(6))
				if err != nil || !slices.Equal(ids(res.Candidates), []int{0, 2, 4}) || res.Stages[0].ItemErrors != 3 || !res.Degraded {
					t.Fatalf("DropItem: %v %+v", err, res)
				}
				// KeepItem
				p = mustNew(t, ScoreEach("s", fn, WithConcurrency(conc), WithItemErrorPolicy(KeepItem)))
				res, err = p.Run(context.Background(), req{}, items(6))
				if err != nil || len(res.Candidates) != 6 || res.Candidates[1].Score != 1 || res.Candidates[2].Score != 100 {
					t.Fatalf("KeepItem: %v %+v", err, res.Candidates)
				}
				// AssignScore
				p = mustNew(t, ScoreEach("s", fn, WithConcurrency(conc), WithItemErrorPolicy(AssignScore(-1))))
				res, err = p.Run(context.Background(), req{}, items(6))
				if err != nil || len(res.Candidates) != 6 || res.Candidates[1].Score != -1 || res.Candidates[2].Score != 100 {
					t.Fatalf("AssignScore: %v %+v", err, res.Candidates)
				}
			})
		}
	}
}

func TestFilterItemPolicies(t *testing.T) {
	fn := func(_ context.Context, _ req, c Candidate[int]) (bool, error) {
		if c.Item == 2 {
			return false, errors.New("lookup failed")
		}
		return c.Item != 4, nil
	}
	for _, conc := range []int{0, 3} {
		p := mustNew(t, Filter("f", fn, WithConcurrency(conc)))
		_, err := p.Run(context.Background(), req{}, items(6))
		var e *Error
		if !errors.As(err, &e) || e.ItemIndex != 2 || e.Op != OpFilter {
			t.Fatalf("FailStage: %v", err)
		}
		p = mustNew(t, Filter("f", fn, WithConcurrency(conc), WithItemErrorPolicy(DropItem)))
		res, _ := p.Run(context.Background(), req{}, items(6))
		if !slices.Equal(ids(res.Candidates), []int{0, 1, 3, 5}) || res.Stages[0].ItemErrors != 1 {
			t.Fatalf("DropItem: %+v", res)
		}
		p = mustNew(t, Filter("f", fn, WithConcurrency(conc), WithItemErrorPolicy(KeepItem)))
		res, _ = p.Run(context.Background(), req{}, items(6))
		if !slices.Equal(ids(res.Candidates), []int{0, 1, 2, 3, 5}) {
			t.Fatalf("KeepItem: %+v", res)
		}
	}
}

func TestScoreBatch(t *testing.T) {
	t.Run("length mismatch is a stage error", func(t *testing.T) {
		p := mustNew(t, ScoreBatch("b", func(_ context.Context, _ req, in []Candidate[int]) ([]float64, error) {
			return make([]float64, len(in)-1), nil
		}, WithItemErrorPolicy(DropItem)))
		_, err := p.Run(context.Background(), req{}, items(4))
		if !errors.Is(err, ErrBatchSizeMismatch) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("sub-batches preserve positions", func(t *testing.T) {
		var calls atomic.Int64
		p := mustNew(t, ScoreBatch("b", func(_ context.Context, _ req, in []Candidate[int]) ([]float64, error) {
			calls.Add(1)
			if cap(in) != len(in) {
				t.Errorf("batch capacity not clamped: len %d cap %d", len(in), cap(in))
			}
			out := make([]float64, len(in))
			for i, c := range in {
				out[i] = float64(c.Item * 10)
			}
			return out, nil
		}, WithMaxBatchSize(7), WithConcurrency(3)))
		res, err := p.Run(context.Background(), req{}, items(50))
		if err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 8 {
			t.Fatalf("calls %d", calls.Load())
		}
		for i, c := range res.Candidates {
			if c.Item != i || c.Score != float64(i*10) {
				t.Fatalf("position %d: %+v", i, c)
			}
		}
	})
	t.Run("one sub-batch fails the stage", func(t *testing.T) {
		p := mustNew(t, ScoreBatch("b", func(_ context.Context, _ req, in []Candidate[int]) ([]float64, error) {
			if in[0].Item == 20 {
				return nil, errors.New("shard down")
			}
			return make([]float64, len(in)), nil
		}, WithMaxBatchSize(10), WithConcurrency(2), WithItemErrorPolicy(DropItem)))
		_, err := p.Run(context.Background(), req{}, items(50))
		if err == nil || !strings.Contains(err.Error(), "sub-batch 2 [20,30)") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("nan follows item policy", func(t *testing.T) {
		fn := func(_ context.Context, _ req, in []Candidate[int]) ([]float64, error) {
			out := make([]float64, len(in))
			for i, c := range in {
				out[i] = 1
				if c.Item == 1 {
					out[i] = math.NaN()
				}
			}
			return out, nil
		}
		_, err := mustNew(t, ScoreBatch("b", fn)).Run(context.Background(), req{}, items(3))
		var e *Error
		if !errors.Is(err, ErrNaNScore) || !errors.As(err, &e) || e.ItemIndex != 1 {
			t.Fatalf("got %v", err)
		}
		res, err := mustNew(t, ScoreBatch("b", fn, WithItemErrorPolicy(DropItem))).Run(context.Background(), req{}, items(3))
		if err != nil || !slices.Equal(ids(res.Candidates), []int{0, 2}) {
			t.Fatalf("got %v %v", res.Candidates, err)
		}
		res, err = mustNew(t, ScoreBatch("b", fn, WithItemErrorPolicy(AssignScore(-9)))).Run(context.Background(), req{}, items(3))
		if err != nil || res.Candidates[1].Score != -9 || res.Candidates[0].Score != 1 {
			t.Fatalf("got %v %v", res.Candidates, err)
		}
	})
}

func TestRerankCannotGrow(t *testing.T) {
	p := mustNew(t, Rerank("r", func(_ context.Context, _ req, in []Candidate[int]) ([]Candidate[int], error) {
		return append(in, Candidate[int]{}), nil
	}))
	_, err := p.Run(context.Background(), req{}, items(2))
	if !errors.Is(err, ErrRerankGrew) {
		t.Fatalf("got %v", err)
	}
	// Removing is fine.
	p = mustNew(t, Rerank("r", func(_ context.Context, _ req, in []Candidate[int]) ([]Candidate[int], error) {
		return in[:1], nil
	}))
	res, err := p.Run(context.Background(), req{}, items(2))
	if err != nil || len(res.Candidates) != 1 {
		t.Fatalf("got %v %v", res.Candidates, err)
	}
}

func TestNilOutputIsEmpty(t *testing.T) {
	p := mustNew(t, Transform("t", func(_ context.Context, _ req, _ []Candidate[int]) ([]Candidate[int], error) {
		return nil, nil
	}))
	res, err := p.Run(context.Background(), req{}, items(2))
	if err != nil || res.Candidates == nil || len(res.Candidates) != 0 || res.Stages[0].Out != 0 {
		t.Fatalf("got %#v %v", res, err)
	}
}

// --- cancellation ---------------------------------------------------------

func TestCancelledBeforeRun(t *testing.T) {
	called := false
	p := mustNew(t, ScoreEach("s", func(_ context.Context, _ req, _ Candidate[int]) (float64, error) {
		called = true
		return 0, nil
	}, WithErrorPolicy(ContinueWithInput)))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.Run(ctx, req{}, items(3))
	var e *Error
	if !errors.Is(err, context.Canceled) || !errors.As(err, &e) || e.Op != OpRun || e.Stage != "s" || called {
		t.Fatalf("got %v called=%v", err, called)
	}
}

func TestStageTimeoutTriggersPolicyButParentDeadlineFailsPipeline(t *testing.T) {
	slow := func(ctx context.Context, _ req, c Candidate[int]) (float64, error) {
		select {
		case <-time.After(200 * time.Millisecond):
			return 1, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	// Stage timeout only: policy applies.
	p := mustNew(t,
		ScoreEach("slow", slow, WithTimeout(10*time.Millisecond), WithErrorPolicy(ContinueWithInput)),
		TopK[req, int]("k", 2),
	)
	res, err := p.Run(context.Background(), req{}, items(5))
	if err != nil || !res.Degraded || res.Stages[0].Status != StatusDegraded || !errors.Is(res.Stages[0].Err, context.DeadlineExceeded) {
		t.Fatalf("stage timeout: err=%v res=%+v", err, res)
	}
	if got := ids(res.Candidates); !slices.Equal(got, []int{4, 3}) {
		t.Fatalf("got %v", got)
	}

	// Parent deadline: policy does NOT apply.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	res, err = p.Run(ctx, req{}, items(5))
	if !errors.Is(err, context.DeadlineExceeded) || res.Candidates != nil || res.Stages[0].Status != StatusFailed {
		t.Fatalf("parent deadline: err=%v res=%+v", err, res)
	}
}

func TestParentCancelPreservedWhenStageReturnsOtherError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p := mustNew(t, ScoreEach("s", func(_ context.Context, _ req, _ Candidate[int]) (float64, error) {
		cancel()
		return 0, errors.New("rpc: unavailable")
	}, WithErrorPolicy(ContinueWithInput)))
	_, err := p.Run(ctx, req{}, items(3))
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "rpc: unavailable") {
		t.Fatalf("got %v", err)
	}
}

func TestCancellationDuringParallelScoringStopsWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var started, finished atomic.Int64
	p := mustNew(t, ScoreEach("s", func(ctx context.Context, _ req, c Candidate[int]) (float64, error) {
		started.Add(1)
		defer finished.Add(1)
		if c.Item == 0 {
			cancel()
			return 0, ctx.Err()
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(50 * time.Millisecond):
			return 1, nil
		}
	}, WithConcurrency(8), WithItemErrorPolicy(DropItem)))
	_, err := p.Run(ctx, req{}, items(10_000))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if started.Load() != finished.Load() {
		t.Fatalf("goroutines leaked: started %d finished %d", started.Load(), finished.Load())
	}
	if started.Load() > 2000 {
		t.Fatalf("workers did not stop promptly: %d items started", started.Load())
	}
}

func TestParallelPartialWorkIsNotReportedAsComplete(t *testing.T) {
	// Scorer ignores ctx (worst case) but the parent is cancelled mid-way:
	// forEach must still fail rather than return the partially scored slice.
	ctx, cancel := context.WithCancel(context.Background())
	var n atomic.Int64
	p := mustNew(t, ScoreEach("s", func(_ context.Context, _ req, _ Candidate[int]) (float64, error) {
		if n.Add(1) == 100 {
			cancel()
		}
		return 1, nil
	}, WithConcurrency(4)))
	_, err := p.Run(ctx, req{}, items(50_000))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

// --- limiter --------------------------------------------------------------

func TestLimiter(t *testing.T) {
	lim := NewLimiter(2)
	var inflight, peak atomic.Int64
	p := mustNew(t, ScoreBatch("b", func(ctx context.Context, _ req, in []Candidate[int]) ([]float64, error) {
		cur := inflight.Add(1)
		for {
			old := peak.Load()
			if cur <= old || peak.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		inflight.Add(-1)
		return make([]float64, len(in)), nil
	}, WithLimiter(lim)))
	done := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func() {
			_, err := p.Run(context.Background(), req{}, items(3))
			done <- err
		}()
	}
	for i := 0; i < 16; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if peak.Load() > 2 {
		t.Fatalf("limiter breached: peak %d", peak.Load())
	}
	// Saturated limiter + stage timeout: fails the stage (and falls back) rather than queueing.
	if err := lim.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := lim.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	p = mustNew(t, ScoreBatch("b", func(context.Context, req, []Candidate[int]) ([]float64, error) {
		t.Fatal("must not run")
		return nil, nil
	}, WithLimiter(lim), WithTimeout(5*time.Millisecond), WithErrorPolicy(ContinueWithInput)))
	res, err := p.Run(context.Background(), req{}, items(3))
	if err != nil || !res.Degraded || !errors.Is(res.Stages[0].Err, context.DeadlineExceeded) {
		t.Fatalf("err=%v res=%+v", err, res)
	}
	lim.Release()
	lim.Release()
}

// --- errors & config ------------------------------------------------------

func TestErrorFormatting(t *testing.T) {
	e := &Error{Stage: "deep", Index: 3, Op: OpScoreBatch, ItemIndex: -1, Err: errors.New("boom")}
	if got := e.Error(); got != `rankpipe: stage "deep" (index 3, score-batch): boom` {
		t.Fatal(got)
	}
	e.ItemIndex = 7
	if got := e.Error(); got != `rankpipe: stage "deep" (index 3, score-batch) item 7: boom` {
		t.Fatal(got)
	}
}

func TestCustomStage(t *testing.T) {
	p := mustNew(t, customStage{})
	res, err := p.Run(context.Background(), req{}, items(3))
	if err != nil || len(res.Candidates) != 1 || res.Stages[0].Name != "custom" {
		t.Fatalf("%v %+v", err, res)
	}
	// Custom stage errors are wrapped with OpCustom.
	p = mustNew(t, customStage{fail: true})
	_, err = p.Run(context.Background(), req{}, items(3))
	var e *Error
	if !errors.As(err, &e) || e.Op != OpCustom || e.Stage != "custom" {
		t.Fatalf("%v", err)
	}
	// Wrapping a custom stage with Transform gives it options.
	p = mustNew(t, Transform("wrapped", customStage{fail: true}.Run, WithErrorPolicy(ContinueWithInput)))
	res, err = p.Run(context.Background(), req{}, items(3))
	if err != nil || len(res.Candidates) != 3 || !res.Degraded {
		t.Fatalf("%v %+v", err, res)
	}
}

type customStage struct{ fail bool }

func (customStage) Name() string { return "custom" }
func (c customStage) Run(_ context.Context, _ req, in []Candidate[int]) ([]Candidate[int], error) {
	if c.fail {
		return nil, errors.New("custom failure")
	}
	return in[:1], nil
}

func TestNewValidation(t *testing.T) {
	ok := func(context.Context, req, Candidate[int]) (float64, error) { return 0, nil }
	okf := func(context.Context, req, Candidate[int]) (bool, error) { return true, nil }
	cases := map[string][]Stage[req, int]{
		"no stages":               {},
		"nil stage":               {nil},
		"empty name":              {ScoreEach("", ok)},
		"duplicate name":          {ScoreEach("a", ok), ScoreEach("a", ok)},
		"duplicate fallback name": {ScoreEach("a", ok, WithFallback(ScoreEach("b", ok))), ScoreEach("b", ok)},
		"nil func":                {ScoreEach[req, int]("a", nil)},
		"negative k":              {TopK[req, int]("k", -1)},
		"concurrency on topk":     {TopK[req, int]("k", 1, WithConcurrency(2))},
		"negative concurrency":    {ScoreEach("a", ok, WithConcurrency(-1))},
		"batch size on scoreeach": {ScoreEach("a", ok, WithMaxBatchSize(3))},
		"nan policy on scoreeach": {ScoreEach("a", ok, WithNaN(NaNDrop))},
		"item policy on topk":     {TopK[req, int]("k", 1, WithItemErrorPolicy(DropItem))},
		"assign score on filter":  {Filter("f", okf, WithItemErrorPolicy(AssignScore(1)))},
		"fallback + continue":     {ScoreEach("a", ok, WithFallback(ScoreEach("b", ok)), WithErrorPolicy(ContinueWithInput))},
		"fallback wrong type":     {ScoreEach("a", ok, WithFallback(ScoreEach("b", func(context.Context, string, Candidate[int]) (float64, error) { return 0, nil })))},
		"invalid fallback":        {ScoreEach("a", ok, WithFallback(TopK[req, int]("b", -1)))},
		"negative timeout":        {ScoreEach("a", ok, WithTimeout(-1))},
		"nil limiter":             {ScoreEach("a", ok, WithLimiter(nil))},
		"unknown nan policy":      {TopK[req, int]("k", 1, WithNaN(NaNPolicy(9)))},
		"unknown error policy":    {ScoreEach("a", ok, WithErrorPolicy(ErrorPolicy(9)))},
	}
	for name, stages := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := New(stages...); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("want ErrInvalidConfig, got %v", err)
			}
		})
	}
	// Valid: same fallback name used by a different pipeline is fine; nil options skipped.
	if _, err := New(ScoreEach("a", ok, nil, WithConcurrency(0)), TopK[req, int]("k", 0)); err != nil {
		t.Fatal(err)
	}
}

func TestStagesNames(t *testing.T) {
	p := mustNew(t, TopK[req, int]("a", 1), TopK[req, int]("b", 1))
	if got := p.Stages(); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatal(got)
	}
}

func TestPolicyStrings(t *testing.T) {
	for _, s := range []fmt.Stringer{FailPipeline, ContinueWithInput, FailStage, DropItem, KeepItem, AssignScore(1.5), NaNError, NaNLowest, NaNDrop, StatusOK, StatusFellBack} {
		if strings.Contains(s.String(), "?") {
			t.Fatal(s.String())
		}
	}
	if AssignScore(math.NaN()).String() != "AssignScore(NaN)" {
		t.Fatal(AssignScore(math.NaN()).String())
	}
}

func TestNewDoesNotMutateSharedStages(t *testing.T) {
	ok := func(context.Context, req, Candidate[int]) (float64, error) { return 0, nil }
	shared := ScoreEach("a", ok, WithFallback(customStage{}))
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := New(shared, TopK[req, int]("k", 1)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
