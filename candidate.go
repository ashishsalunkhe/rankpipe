package rankpipe

// Candidate is one item flowing through a pipeline together with its current
// score. It is a small value type: rankpipe moves Candidate values when it
// compacts, sorts and copies, so keep T small (an ID, a pointer, or a small
// struct).
//
// Score is the single canonical score. Scoring stages overwrite it; a scorer
// receives the candidate including its current score, so combining a previous
// score with a new one is ordinary user code. rankpipe never interprets the
// value beyond "larger is better".
type Candidate[T any] struct {
	Item  T
	Score float64
}

// Candidates wraps items into candidates with a zero score, preserving order.
func Candidates[T any](items []T) []Candidate[T] {
	if items == nil {
		return nil
	}
	out := make([]Candidate[T], len(items))
	for i, it := range items {
		out[i].Item = it
	}
	return out
}

// Items extracts the items from candidates, preserving order.
func Items[T any](cs []Candidate[T]) []T {
	if cs == nil {
		return nil
	}
	out := make([]T, len(cs))
	for i := range cs {
		out[i] = cs[i].Item
	}
	return out
}
