package rankpipe

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"
)

func benchCandidates(n int) []Candidate[int] {
	r := rand.New(rand.NewPCG(7, 11))
	in := make([]Candidate[int], n)
	for i := range in {
		in[i] = Candidate[int]{Item: i, Score: r.Float64()}
	}
	return in
}

func BenchmarkTopK(b *testing.B) {
	for _, n := range []int{1_000, 100_000} {
		in := benchCandidates(n)
		for _, k := range []int{10, 100, n / 10, n / 2, n * 3 / 4, n} {
			if k == 0 {
				continue
			}
			b.Run(fmt.Sprintf("n=%d/k=%d/heap", n, k), func(b *testing.B) {
				if k >= n {
					b.Skip()
				}
				work := make([]Candidate[int], n)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					copy(work, in)
					topKHeap(work, k)
				}
			})
			b.Run(fmt.Sprintf("n=%d/k=%d/sort-refs", n, k), func(b *testing.B) {
				work := make([]Candidate[int], n)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					copy(work, in)
					topKSortRefs(work, k)
				}
			})
		}
	}
}

func BenchmarkScoreEach(b *testing.B) {
	cheap := func(_ context.Context, _ req, c Candidate[int]) (float64, error) {
		return float64(c.Item%13) * 0.5, nil
	}
	spin := func(_ context.Context, _ req, c Candidate[int]) (float64, error) {
		// ~1µs of CPU work.
		x := c.Score
		for i := 0; i < 300; i++ {
			x = x*1.000001 + 0.5
		}
		return x, nil
	}
	sleepy := func(_ context.Context, _ req, c Candidate[int]) (float64, error) {
		time.Sleep(50 * time.Microsecond) // stands in for an RPC
		return c.Score, nil
	}
	for _, sc := range []struct {
		name string
		fn   ScoreFunc[req, int]
		n    int
	}{{"cheap", cheap, 100_000}, {"1us", spin, 10_000}, {"50us-sleep", sleepy, 1_000}} {
		for _, conc := range []int{1, 4, 16} {
			p, _ := New(ScoreEach("s", sc.fn, WithConcurrency(conc)))
			in := benchCandidates(sc.n)
			b.Run(fmt.Sprintf("%s/n=%d/conc=%d", sc.name, sc.n, conc), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					if _, err := p.Run(context.Background(), req{}, in); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkFilter(b *testing.B) {
	p, _ := New(Filter("f", func(_ context.Context, _ req, c Candidate[int]) (bool, error) { return c.Item%3 != 0, nil }))
	in := benchCandidates(100_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		p.Run(context.Background(), req{}, in)
	}
}

// BenchmarkFunnel is the representative 50k → 20 funnel from the README with
// cheap in-process callbacks: it measures rankpipe's own overhead.
func BenchmarkFunnel(b *testing.B) {
	p, _ := New(
		Filter("eligible", func(_ context.Context, _ req, c Candidate[int]) (bool, error) { return c.Item%4 != 0, nil }),
		ScoreEach("light", func(_ context.Context, _ req, c Candidate[int]) (float64, error) {
			return c.Score * float64(c.Item%7), nil
		}),
		TopK[req, int]("light-top-k", 1000),
		ScoreBatch("deep", func(_ context.Context, _ req, in []Candidate[int]) ([]float64, error) {
			out := make([]float64, len(in))
			for i, c := range in {
				out[i] = c.Score + 1
			}
			return out, nil
		}, WithMaxBatchSize(256)),
		TopK[req, int]("deep-top-k", 100),
		Rerank("diversity", func(_ context.Context, _ req, in []Candidate[int]) ([]Candidate[int], error) { return in, nil }),
		TopK[req, int]("final", 20),
	)
	in := benchCandidates(50_000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := p.Run(context.Background(), req{}, in); err != nil {
			b.Fatal(err)
		}
	}
}
