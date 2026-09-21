package rankpipe

import (
	"context"
	"errors"
	"slices"
	"time"
)

// Pipeline is an immutable, ordered list of stages. Build one with New at
// start-up and call Run from any number of goroutines.
type Pipeline[Req, T any] struct {
	stages []*stage[Req, T]
}

// Status describes how a stage concluded in a StageReport.
type Status int

const (
	// StatusOK: the stage completed. Tolerated item errors, if any, are counted
	// in StageReport.ItemErrors.
	StatusOK Status = iota
	// StatusFailed: the stage failed and the pipeline stopped.
	StatusFailed
	// StatusDegraded: the stage failed and the pipeline continued with the
	// stage's input (ContinueWithInput).
	StatusDegraded
	// StatusFellBack: the stage failed and its fallback stage ran; the
	// fallback's own report follows with Fallback set.
	StatusFellBack
)

func (s Status) String() string {
	switch s {
	case StatusOK:
		return "ok"
	case StatusFailed:
		return "failed"
	case StatusDegraded:
		return "degraded"
	case StatusFellBack:
		return "fell-back"
	}
	return "status(?)"
}

// StageReport records what one stage invocation did. Reports are the
// pipeline's debugging and metrics surface: emit them after Run.
type StageReport struct {
	Name       string
	Index      int  // index of the (primary) stage in the pipeline
	Fallback   bool // this report is for a fallback stage run in place of Index
	In, Out    int  // candidate counts entering and leaving the stage
	Duration   time.Duration
	Status     Status
	ItemErrors int   // per-candidate failures tolerated by the item error policy
	Err        error // the stage's error, if any, even when a policy absorbed it
}

// Result is the outcome of a Run.
type Result[T any] struct {
	// Candidates is the final list. It is nil when Run returns an error.
	Candidates []Candidate[T]
	// Degraded is true when any non-default failure policy was exercised:
	// a stage continued with its input, a fallback ran, or item errors were
	// tolerated. The details are in Stages.
	Degraded bool
	// Stages holds one report per stage invocation in execution order,
	// including fallbacks. On error it holds the reports up to and including
	// the failing stage.
	Stages []StageReport
}

// New validates the stages and their options and returns an immutable
// pipeline. Stage names must be non-empty and unique (fallback stages
// included). Options that do not apply to a stage are configuration errors.
func New[Req, T any](stages ...Stage[Req, T]) (*Pipeline[Req, T], error) {
	if len(stages) == 0 {
		return nil, configErr("pipeline has no stages")
	}
	p := &Pipeline[Req, T]{stages: make([]*stage[Req, T], 0, len(stages))}
	names := make(map[string]struct{}, len(stages))
	for i, st := range stages {
		if st == nil {
			return nil, configErr("stage %d is nil", i)
		}
		s, err := asStage(st).resolve()
		if err != nil {
			return nil, err
		}
		for f := s; f != nil; f = f.fallback {
			if _, dup := names[f.name]; dup {
				return nil, configErr("duplicate stage name %q", f.name)
			}
			names[f.name] = struct{}{}
		}
		p.stages = append(p.stages, s)
	}
	return p, nil
}

// Stages returns the names of the pipeline's primary stages in order.
func (p *Pipeline[Req, T]) Stages() []string {
	out := make([]string, len(p.stages))
	for i, s := range p.stages {
		out[i] = s.name
	}
	return out
}

// Run executes the pipeline. It never modifies in; the caller may reuse it
// immediately. The returned candidates do not alias in.
//
// If ctx is done before a stage starts, Run fails with that stage's name and
// the context error as cause. If a stage fails while the parent context is
// done, the pipeline fails regardless of the stage's error policy.
func (p *Pipeline[Req, T]) Run(ctx context.Context, req Req, in []Candidate[T]) (Result[T], error) {
	res := Result[T]{Stages: make([]StageReport, 0, len(p.stages))}
	work := slices.Clone(in)
	if work == nil {
		work = []Candidate[T]{}
	}
	for i, s := range p.stages {
		if err := ctx.Err(); err != nil {
			return res, &Error{Stage: s.name, Index: i, Op: OpRun, ItemIndex: -1, Err: err}
		}
		var err error
		work, err = p.runStage(ctx, i, s, false, req, work, &res)
		if err != nil {
			return res, err
		}
	}
	res.Candidates = work
	return res, nil
}

// runStage executes one stage with its error policy, fallback and reporting.
func (p *Pipeline[Req, T]) runStage(ctx context.Context, idx int, s *stage[Req, T], isFallback bool, req Req, in []Candidate[T], res *Result[T]) ([]Candidate[T], error) {
	// Policies that reuse the input need it intact; primitives mutate in place.
	input := in
	if s.cfg.policy == ContinueWithInput || s.fallback != nil {
		input = slices.Clone(in)
	}

	start := time.Now()
	out, st, err := s.exec(ctx, req, in)
	rep := StageReport{
		Name:       s.name,
		Index:      idx,
		Fallback:   isFallback,
		In:         len(in),
		Duration:   time.Since(start),
		ItemErrors: st.itemErrors,
	}
	if st.itemErrors > 0 {
		res.Degraded = true
	}
	if err == nil {
		if out == nil {
			out = in[:0]
		}
		rep.Out = len(out)
		rep.Status = StatusOK
		res.Stages = append(res.Stages, rep)
		return out, nil
	}

	werr := wrapErr(s, idx, err)
	rep.Err = werr

	// A dead parent context overrides every policy: nothing downstream can run.
	if cerr := ctx.Err(); cerr != nil {
		if !errors.Is(err, cerr) {
			werr.Err = errors.Join(cerr, err)
		}
		rep.Status = StatusFailed
		res.Stages = append(res.Stages, rep)
		return nil, werr
	}

	switch {
	case s.fallback != nil:
		rep.Status = StatusFellBack
		rep.Out = len(input)
		res.Stages = append(res.Stages, rep)
		res.Degraded = true
		return p.runStage(ctx, idx, s.fallback, true, req, input, res)
	case s.cfg.policy == ContinueWithInput:
		rep.Status = StatusDegraded
		rep.Out = len(input)
		res.Stages = append(res.Stages, rep)
		res.Degraded = true
		return input, nil
	default:
		rep.Status = StatusFailed
		res.Stages = append(res.Stages, rep)
		return nil, werr
	}
}

func wrapErr[Req, T any](s *stage[Req, T], idx int, err error) *Error {
	e := &Error{Stage: s.name, Index: idx, Op: s.kind.op(), ItemIndex: -1, Err: err}
	var ie *itemError
	if errors.As(err, &ie) {
		e.ItemIndex = ie.index
		e.Err = ie.err
	}
	return e
}
