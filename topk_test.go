package rankpipe

import (
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"testing"
)

// refTopK is the deliberately simple reference: apply the NaN policy, stable
// sort by score descending, truncate.
func refTopK[T any](in []Candidate[T], k int, nan NaNPolicy) ([]Candidate[T], error) {
	out := slices.Clone(in)
	if k == 0 {
		return out[:0], nil
	}
	switch nan {
	case NaNError:
		for _, c := range out {
			if math.IsNaN(c.Score) {
				return nil, ErrNaNScore
			}
		}
	case NaNDrop:
		out = slices.DeleteFunc(out, func(c Candidate[T]) bool { return math.IsNaN(c.Score) })
	}
	slices.SortStableFunc(out, func(a, b Candidate[T]) int {
		an, bn := math.IsNaN(a.Score), math.IsNaN(b.Score)
		switch {
		case an && bn:
			return 0
		case an:
			return 1
		case bn:
			return -1
		case a.Score > b.Score:
			return -1
		case a.Score < b.Score:
			return 1
		}
		return 0
	})
	if k < len(out) {
		out = out[:k]
	}
	return out, nil
}

// scorePool is chosen to produce many ties and every IEEE-754 edge case.
var scorePool = []float64{
	-2, -1, math.Copysign(0, -1), 0, 0.5, 1, 2, 1e300, -1e300,
	math.Inf(1), math.Inf(-1),
}

func randomCandidates(r *rand.Rand, n int, withNaN bool) []Candidate[int] {
	in := make([]Candidate[int], n)
	for i := range in {
		in[i].Item = i
		if withNaN && r.IntN(7) == 0 {
			in[i].Score = math.NaN()
		} else {
			in[i].Score = scorePool[r.IntN(len(scorePool))]
		}
	}
	return in
}

func sameCandidates(a, b []Candidate[int]) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Item != b[i].Item {
			return false
		}
		// Item identity implies score identity (scores are never rewritten by
		// topK), but check bit patterns anyway so -0 vs +0 is not conflated.
		if math.Float64bits(a[i].Score) != math.Float64bits(b[i].Score) {
			return false
		}
	}
	return true
}

func TestTopKProperty(t *testing.T) {
	policies := []NaNPolicy{NaNError, NaNLowest, NaNDrop}
	for seed := uint64(1); seed <= 400; seed++ {
		r := rand.New(rand.NewPCG(seed, seed*7919))
		n := r.IntN(300)
		k := r.IntN(n + 6)
		withNaN := r.IntN(2) == 0
		in := randomCandidates(r, n, withNaN)
		for _, pol := range policies {
			want, wantErr := refTopK(in, k, pol)
			got, gotErr := topK(slices.Clone(in), k, pol)
			if !errors.Is(gotErr, wantErr) && (gotErr == nil) != (wantErr == nil) {
				t.Fatalf("seed %d n=%d k=%d %v: err got %v want %v", seed, n, k, pol, gotErr, wantErr)
			}
			if gotErr != nil {
				continue
			}
			if !sameCandidates(got, want) {
				t.Fatalf("seed %d n=%d k=%d %v:\n got %v\nwant %v", seed, n, k, pol, got, want)
			}
		}
	}
}

// TestTopKBothPaths forces both selection strategies over the full K range so
// the crossover constant cannot hide a bug in either.
func TestTopKBothPaths(t *testing.T) {
	r := rand.New(rand.NewPCG(42, 43))
	for _, n := range []int{1, 2, 3, 7, 64, 1000, 5000} {
		in := randomCandidates(r, n, false)
		for _, k := range []int{1, 2, n / 3, n / 2, n - 1, n} {
			if k < 1 || k > n {
				continue
			}
			want, _ := refTopK(in, k, NaNError)
			heap := topKHeap(slices.Clone(in), k)
			if !sameCandidates(heap, want) {
				t.Fatalf("heap n=%d k=%d mismatch", n, k)
			}
			sorted := topKSortRefs(slices.Clone(in), k)
			if !sameCandidates(sorted, want) {
				t.Fatalf("sort-refs n=%d k=%d mismatch", n, k)
			}
		}
	}
}

func TestTopKEdges(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		out, err := topK([]Candidate[int]{}, 5, NaNError)
		if err != nil || len(out) != 0 {
			t.Fatalf("got %v, %v", out, err)
		}
	})
	t.Run("k=0", func(t *testing.T) {
		out, err := topK([]Candidate[int]{{1, 1}, {2, 2}}, 0, NaNError)
		if err != nil || len(out) != 0 {
			t.Fatalf("got %v, %v", out, err)
		}
	})
	t.Run("k>n sorts", func(t *testing.T) {
		out, err := topK([]Candidate[int]{{1, 1}, {2, 3}, {3, 2}}, 10, NaNError)
		if err != nil || !sameCandidates(out, []Candidate[int]{{2, 3}, {3, 2}, {1, 1}}) {
			t.Fatalf("got %v, %v", out, err)
		}
	})
	t.Run("ties keep input order", func(t *testing.T) {
		in := []Candidate[int]{{1, 1}, {2, 1}, {3, 2}, {4, 1}, {5, 2}}
		out, _ := topK(slices.Clone(in), 3, NaNError)
		if !sameCandidates(out, []Candidate[int]{{3, 2}, {5, 2}, {1, 1}}) {
			t.Fatalf("got %v", out)
		}
	})
	t.Run("negative zero equals zero", func(t *testing.T) {
		in := []Candidate[int]{{1, 0}, {2, math.Copysign(0, -1)}, {3, 0}}
		out, _ := topK(slices.Clone(in), 2, NaNError)
		if out[0].Item != 1 || out[1].Item != 2 {
			t.Fatalf("got %v", out)
		}
	})
	t.Run("inf", func(t *testing.T) {
		in := []Candidate[int]{{1, math.Inf(-1)}, {2, 5}, {3, math.Inf(1)}}
		out, _ := topK(slices.Clone(in), 3, NaNError)
		if out[0].Item != 3 || out[1].Item != 2 || out[2].Item != 1 {
			t.Fatalf("got %v", out)
		}
	})
	t.Run("nan error", func(t *testing.T) {
		_, err := topK([]Candidate[int]{{1, 1}, {2, math.NaN()}}, 1, NaNError)
		if !errors.Is(err, ErrNaNScore) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("nan lowest", func(t *testing.T) {
		in := []Candidate[int]{{1, math.NaN()}, {2, math.Inf(-1)}, {3, math.NaN()}}
		out, err := topK(slices.Clone(in), 3, NaNLowest)
		if err != nil || out[0].Item != 2 || out[1].Item != 1 || out[2].Item != 3 {
			t.Fatalf("got %v, %v", out, err)
		}
	})
	t.Run("nan drop", func(t *testing.T) {
		in := []Candidate[int]{{1, math.NaN()}, {2, 1}, {3, math.NaN()}}
		out, err := topK(slices.Clone(in), 3, NaNDrop)
		if err != nil || len(out) != 1 || out[0].Item != 2 {
			t.Fatalf("got %v, %v", out, err)
		}
	})
	t.Run("all nan dropped", func(t *testing.T) {
		out, err := topK([]Candidate[int]{{1, math.NaN()}}, 3, NaNDrop)
		if err != nil || len(out) != 0 {
			t.Fatalf("got %v, %v", out, err)
		}
	})
}

func TestWorseIsTotalOrder(t *testing.T) {
	vals := append(slices.Clone(scorePool), math.NaN())
	var refs []ref
	for i, v := range vals {
		refs = append(refs, ref{v, i}, ref{v, i + 100})
	}
	for _, a := range refs {
		if worse(a, a) {
			t.Fatalf("irreflexivity violated for %v", a)
		}
		for _, b := range refs {
			same := a.idx == b.idx && math.Float64bits(a.score) == math.Float64bits(b.score)
			if !same && worse(a, b) == worse(b, a) {
				t.Fatalf("asymmetry/totality violated for %v, %v", a, b)
			}
			for _, c := range refs {
				if worse(a, b) && worse(b, c) && !worse(a, c) {
					t.Fatalf("transitivity violated for %v %v %v", a, b, c)
				}
			}
		}
	}
}
