package rankpipe

import (
	"slices"
)

// ref is a candidate reference used during heap selection so that large T is
// never moved while selecting.
type ref struct {
	score float64
	idx   int
}

// worse reports whether a ranks strictly below b under the pipeline's total
// order: higher score first, then lower index (earlier input) first. NaN,
// when present, ranks below everything (NaNLowest); the other NaN policies
// remove or reject NaN before comparison.
func worse(a, b ref) bool {
	if a.score < b.score {
		return true
	}
	if a.score > b.score {
		return false
	}
	// Neither < nor >: equal scores (including -0 == +0), or NaN involved.
	an, bn := a.score != a.score, b.score != b.score
	if an != bn {
		return an
	}
	return a.idx > b.idx
}

// topK returns the k best candidates in descending order. See TopK for the
// contract. The result may alias in.
func topK[T any](in []Candidate[T], k int, nan NaNPolicy) ([]Candidate[T], error) {
	n := len(in)
	if k == 0 || n == 0 {
		return in[:0], nil
	}

	if nan != NaNLowest {
		// One cheap pass; the fast path (no NaN) touches nothing else.
		hasNaN := false
		for i := range in {
			if in[i].Score != in[i].Score {
				hasNaN = true
				break
			}
		}
		if hasNaN {
			if nan == NaNError {
				return nil, ErrNaNScore
			}
			out := in[:0]
			for i := range in {
				if in[i].Score == in[i].Score {
					out = append(out, in[i])
				}
			}
			in = out
			n = len(in)
			if n == 0 {
				return in, nil
			}
		}
	}

	// Strategy, chosen by benchmark (BenchmarkTopK): the bounded heap wins or
	// ties at every K < N (it never moves candidates while selecting and does
	// O(N log K) work); when everything must be ordered, a pdqsort over
	// (score, index) references is ~2x faster than slices.SortStableFunc on
	// the candidates themselves and equally deterministic because the
	// comparator is a total order.
	if k < n {
		return topKHeap(in, k), nil
	}
	return topKSortRefs(in, k), nil
}

// topKHeap selects the k best of in (0 < k < len(in)) with a bounded min-heap
// of references: O(n log k) time, 16k bytes of scratch plus the output.
func topKHeap[T any](in []Candidate[T], k int) []Candidate[T] {
	h := make([]ref, k)
	for i := 0; i < k; i++ {
		h[i] = ref{in[i].Score, i}
	}
	// Heapify: the worst element sits at the root.
	for i := k/2 - 1; i >= 0; i-- {
		siftDown(h, i)
	}
	for i := k; i < len(in); i++ {
		c := ref{in[i].Score, i}
		if worse(h[0], c) {
			h[0] = c
			siftDown(h, 0)
		}
	}
	// (score desc, idx asc) is a total order, so an unstable sort is
	// deterministic here.
	slices.SortFunc(h, cmpRef)
	out := make([]Candidate[T], k)
	for i, r := range h {
		out[i] = in[r.idx]
	}
	return out
}

// siftDown restores the heap property below i: the root is the worst ref.
func siftDown(h []ref, i int) {
	n := len(h)
	for {
		l := 2*i + 1
		if l >= n {
			return
		}
		m := l
		if r := l + 1; r < n && worse(h[r], h[l]) {
			m = r
		}
		if !worse(h[m], h[i]) {
			return
		}
		h[i], h[m] = h[m], h[i]
		i = m
	}
}

// cmpRef is the three-way form of worse: best first.
func cmpRef(a, b ref) int {
	if a.score > b.score {
		return -1
	}
	if a.score < b.score {
		return 1
	}
	an, bn := a.score != a.score, b.score != b.score
	if an != bn {
		if an {
			return 1
		}
		return -1
	}
	return a.idx - b.idx
}

// topKSortRefs sorts (score, index) references of every candidate with an
// unstable sort — the comparator is a total order, so the result is still
// deterministic and tie-stable — then gathers the first k. O(n log n) time,
// 16n bytes of scratch plus the output.
func topKSortRefs[T any](in []Candidate[T], k int) []Candidate[T] {
	n := len(in)
	refs := make([]ref, n)
	for i := range in {
		refs[i] = ref{in[i].Score, i}
	}
	slices.SortFunc(refs, cmpRef)
	if k > n {
		k = n
	}
	out := make([]Candidate[T], k)
	for i := 0; i < k; i++ {
		out[i] = in[refs[i].idx]
	}
	return out
}
