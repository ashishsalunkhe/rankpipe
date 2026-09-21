package rankpipe

import (
	"context"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
)

// deterministicPipeline exercises every concurrent path with a scorer whose
// output depends only on its input, heavy ties, and a NaN-producing item that
// is dropped, so that any scheduling-dependent behaviour would show up as a
// differing output.
func deterministicPipeline(t *testing.T) *Pipeline[req, int] {
	t.Helper()
	score := func(_ context.Context, r req, c Candidate[int]) (float64, error) {
		if c.Item%97 == 0 {
			return math.NaN(), nil // dropped by policy
		}
		h := fnv.New32a()
		h.Write([]byte(r.user))
		h.Write([]byte{byte(c.Item), byte(c.Item >> 8)})
		return float64(h.Sum32() % 50), nil // only 50 distinct values → many ties
	}
	batch := func(_ context.Context, _ req, in []Candidate[int]) ([]float64, error) {
		out := make([]float64, len(in))
		for i, c := range in {
			out[i] = c.Score + float64(c.Item%3) // more ties
		}
		return out, nil
	}
	return mustNew(t,
		Filter("odd", func(_ context.Context, _ req, c Candidate[int]) (bool, error) { return c.Item%2 == 1, nil }, WithConcurrency(7)),
		ScoreEach("hash", score, WithConcurrency(16), WithItemErrorPolicy(DropItem)),
		TopK[req, int]("top500", 500),
		ScoreBatch("batch", batch, WithMaxBatchSize(33), WithConcurrency(5)),
		TopK[req, int]("top50", 50),
	)
}

func TestDeterminismRepeated(t *testing.T) {
	p := deterministicPipeline(t)
	r := rand.New(rand.NewPCG(1, 2))
	in := make([]Candidate[int], 20_000)
	for i := range in {
		in[i] = Candidate[int]{Item: r.IntN(5000), Score: float64(r.IntN(10))}
	}
	base, err := p.Run(context.Background(), req{"u"}, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(base.Candidates) != 50 {
		t.Fatalf("got %d", len(base.Candidates))
	}
	for i := 0; i < 200; i++ {
		res, err := p.Run(context.Background(), req{"u"}, in)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(res.Candidates, base.Candidates) {
			t.Fatalf("run %d differs", i)
		}
	}
}

func TestDeterminismConcurrentRuns(t *testing.T) {
	p := deterministicPipeline(t)
	in := items(10_000)
	base, err := p.Run(context.Background(), req{"x"}, in)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan string, 64)
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				res, err := p.Run(context.Background(), req{"x"}, in)
				if err != nil {
					errs <- err.Error()
					return
				}
				if !slices.Equal(res.Candidates, base.Candidates) {
					errs <- "output differs"
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
}
