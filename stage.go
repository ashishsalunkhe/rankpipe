package rankpipe

import (
	"context"
	"math"
	"strconv"
	"time"
)

// Stage is one step of a pipeline. It receives the working candidate slice,
// which it owns for the duration of the call: it may mutate it in place,
// return a sub-slice, or return a new slice. Whatever it returns becomes the
// next stage's input. Stages must be safe for concurrent use.
//
// Built-in stages are constructed with Filter, ScoreEach, ScoreBatch, TopK,
// Rerank and Transform. Any type implementing this interface is accepted by
// New; to attach options (timeout, error policy, …) to a hand-written stage,
// wrap it: Transform(name, custom.Run, opts...).
type Stage[Req, T any] interface {
	// Name identifies the stage in reports and errors. Names must be unique
	// within a pipeline.
	Name() string
	// Run executes the stage.
	Run(ctx context.Context, req Req, in []Candidate[T]) ([]Candidate[T], error)
}

// FilterFunc decides whether a candidate is kept.
type FilterFunc[Req, T any] func(ctx context.Context, req Req, c Candidate[T]) (bool, error)

// ScoreFunc computes a new score for one candidate. It receives the candidate
// with its current score, so combining scores is user code.
type ScoreFunc[Req, T any] func(ctx context.Context, req Req, c Candidate[T]) (float64, error)

// BatchScoreFunc scores a batch. It must return exactly len(in) scores, with
// scores[i] belonging to in[i]; any other length fails the stage with
// ErrBatchSizeMismatch. It must not reorder or modify the slice it is given.
// A NaN score is an item-level error for that candidate.
type BatchScoreFunc[Req, T any] func(ctx context.Context, req Req, in []Candidate[T]) ([]float64, error)

// StageFunc is a list-wise operation over the whole candidate slice, used by
// Rerank and Transform. See those constructors for what each may do.
type StageFunc[Req, T any] func(ctx context.Context, req Req, in []Candidate[T]) ([]Candidate[T], error)

// ErrorPolicy decides what happens to the pipeline when a stage fails.
// It does not apply when the parent context is done: a cancelled or expired
// request always fails the pipeline.
type ErrorPolicy int

const (
	// FailPipeline stops the pipeline and returns the stage error. Default.
	FailPipeline ErrorPolicy = iota
	// ContinueWithInput passes the failed stage's input unchanged to the next
	// stage and marks the result degraded. The input is snapshotted before the
	// stage runs, which costs one copy of the working slice; prefer to place
	// this policy after a TopK.
	ContinueWithInput
)

func (p ErrorPolicy) String() string {
	switch p {
	case FailPipeline:
		return "FailPipeline"
	case ContinueWithInput:
		return "ContinueWithInput"
	}
	return "ErrorPolicy(?)"
}

// ItemErrorPolicy decides what happens to one candidate when a per-candidate
// callback fails (Filter, ScoreEach) or a batch scorer returns NaN for it
// (ScoreBatch). Cancellation is never an item-level error.
type ItemErrorPolicy struct {
	kind  itemPolicyKind
	score float64
}

type itemPolicyKind uint8

const (
	failStage itemPolicyKind = iota
	dropItem
	keepItem
	assignScore
)

var (
	// FailStage fails the stage on the first item error. Default.
	FailStage = ItemErrorPolicy{kind: failStage}
	// DropItem removes the candidate; relative order of the others is preserved.
	DropItem = ItemErrorPolicy{kind: dropItem}
	// KeepItem keeps the candidate: a Filter keeps it, a scorer leaves its
	// previous score untouched. Beware of mixing score scales.
	KeepItem = ItemErrorPolicy{kind: keepItem}
)

// AssignScore keeps the candidate and sets its score to v. Valid for
// ScoreEach and ScoreBatch only.
func AssignScore(v float64) ItemErrorPolicy {
	return ItemErrorPolicy{kind: assignScore, score: v}
}

func (p ItemErrorPolicy) String() string {
	switch p.kind {
	case failStage:
		return "FailStage"
	case dropItem:
		return "DropItem"
	case keepItem:
		return "KeepItem"
	case assignScore:
		return "AssignScore(" + formatFloat(p.score) + ")"
	}
	return "ItemErrorPolicy(?)"
}

// NaNPolicy decides how TopK treats NaN scores.
type NaNPolicy int

const (
	// NaNError fails the stage with ErrNaNScore. Default: a NaN score is almost
	// always an upstream bug, and a silent ordering is worse than an error.
	NaNError NaNPolicy = iota
	// NaNLowest ranks NaN below every other value, including -Inf. Ties among
	// NaNs preserve incoming order.
	NaNLowest
	// NaNDrop removes candidates with NaN scores before selection.
	NaNDrop
)

func (p NaNPolicy) String() string {
	switch p {
	case NaNError:
		return "NaNError"
	case NaNLowest:
		return "NaNLowest"
	case NaNDrop:
		return "NaNDrop"
	}
	return "NaNPolicy(?)"
}

// Option configures a stage. Options are validated when the pipeline is built
// with New; an option that does not apply to the stage it is given to is a
// configuration error rather than silently ignored.
type Option func(*config) error

type config struct {
	concurrency  int
	maxBatch     int
	itemPolicy   ItemErrorPolicy
	itemPolicyOK bool // WithItemErrorPolicy was given
	nan          NaNPolicy
	nanSet       bool
	timeout      time.Duration
	policy       ErrorPolicy
	fallback     any // Stage[Req, T]; asserted in New
	limiter      *Limiter
}

// WithConcurrency runs per-item work (Filter, ScoreEach) or sub-batches
// (ScoreBatch with WithMaxBatchSize) on up to n workers. n <= 1 is sequential,
// the default. Workers claim chunks of indices; no goroutine is created per
// candidate. Concurrency helps I/O-bound callbacks and hurts very cheap ones;
// benchmark before enabling it.
func WithConcurrency(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return configErr("WithConcurrency(%d): must be >= 0", n)
		}
		c.concurrency = n
		return nil
	}
}

// WithMaxBatchSize splits ScoreBatch input into sub-batches of at most n
// candidates, each scored by one call. 0 (the default) means one call for the
// whole input.
func WithMaxBatchSize(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return configErr("WithMaxBatchSize(%d): must be >= 0", n)
		}
		c.maxBatch = n
		return nil
	}
}

// WithItemErrorPolicy sets the per-candidate failure policy for Filter,
// ScoreEach and ScoreBatch. Default: FailStage.
func WithItemErrorPolicy(p ItemErrorPolicy) Option {
	return func(c *config) error {
		c.itemPolicy = p
		c.itemPolicyOK = true
		return nil
	}
}

// WithNaN sets how TopK treats NaN scores. Default: NaNError.
func WithNaN(p NaNPolicy) Option {
	return func(c *config) error {
		if p < NaNError || p > NaNDrop {
			return configErr("WithNaN(%d): unknown policy", int(p))
		}
		c.nan = p
		c.nanSet = true
		return nil
	}
}

// WithTimeout bounds the stage with a child context deadline. The parent
// deadline still applies if it is sooner. Cancellation is cooperative: the
// callbacks must honour their context. If only the stage deadline fires, the
// stage's error policy applies; if the parent context is done, the pipeline
// fails.
func WithTimeout(d time.Duration) Option {
	return func(c *config) error {
		if d < 0 {
			return configErr("WithTimeout(%v): must be >= 0", d)
		}
		c.timeout = d
		return nil
	}
}

// WithErrorPolicy sets the stage-level failure policy. Default: FailPipeline.
// Mutually exclusive with WithFallback.
func WithErrorPolicy(p ErrorPolicy) Option {
	return func(c *config) error {
		if p < FailPipeline || p > ContinueWithInput {
			return configErr("WithErrorPolicy(%d): unknown policy", int(p))
		}
		c.policy = p
		return nil
	}
}

// WithFallback runs s on the failed stage's input when the stage fails and the
// parent context is still live. The fallback is a full stage with its own
// options and appears in the result as a separate report. The failed stage's
// input is snapshotted before it runs (one copy of the working slice); prefer
// to place fallbacks after a TopK. Mutually exclusive with
// WithErrorPolicy(ContinueWithInput).
func WithFallback[Req, T any](s Stage[Req, T]) Option {
	return func(c *config) error {
		if s == nil {
			return configErr("WithFallback(nil)")
		}
		c.fallback = s
		return nil
	}
}

// WithLimiter bounds how many invocations of this stage — across all
// concurrent Run calls sharing l — may execute at once. A permit is held for
// the whole stage invocation. Acquisition honours the (stage-timeout-bounded)
// context, so work that cannot start in time fails the stage instead of
// queueing.
func WithLimiter(l *Limiter) Option {
	return func(c *config) error {
		if l == nil {
			return configErr("WithLimiter(nil)")
		}
		c.limiter = l
		return nil
	}
}

type kind uint8

const (
	kindFilter kind = iota
	kindScoreEach
	kindScoreBatch
	kindTopK
	kindRerank
	kindTransform
	kindCustom
)

func (k kind) op() string {
	switch k {
	case kindFilter:
		return OpFilter
	case kindScoreEach:
		return OpScore
	case kindScoreBatch:
		return OpScoreBatch
	case kindTopK:
		return OpTopK
	case kindRerank:
		return OpRerank
	case kindTransform:
		return OpTransform
	}
	return OpCustom
}

// stage is the concrete type behind every built-in Stage. Configuration is
// applied by the constructor and validated by New; after New nothing mutates it.
type stage[Req, T any] struct {
	name string
	kind kind
	cfg  config
	err  error // first option error, surfaced by New

	filter FilterFunc[Req, T]
	score  ScoreFunc[Req, T]
	batch  BatchScoreFunc[Req, T]
	list   StageFunc[Req, T]
	custom Stage[Req, T]
	k      int

	fallback *stage[Req, T] // resolved by New from cfg.fallback
}

// stats is per-invocation bookkeeping a primitive reports to the runner.
type stats struct {
	itemErrors int
}

func newStage[Req, T any](name string, k kind, opts []Option) *stage[Req, T] {
	s := &stage[Req, T]{name: name, kind: k}
	for _, o := range opts {
		if o == nil {
			continue
		}
		if err := o(&s.cfg); err != nil && s.err == nil {
			s.err = err
		}
	}
	return s
}

// Filter keeps the candidates for which fn returns true, preserving order.
// Item errors follow WithItemErrorPolicy (FailStage, DropItem or KeepItem).
func Filter[Req, T any](name string, fn FilterFunc[Req, T], opts ...Option) Stage[Req, T] {
	s := newStage[Req, T](name, kindFilter, opts)
	s.filter = fn
	return s
}

// ScoreEach scores candidates one at a time with fn, writing each result into
// the candidate's Score. Order is preserved regardless of concurrency. A NaN
// result is an item error. Item errors follow WithItemErrorPolicy.
func ScoreEach[Req, T any](name string, fn ScoreFunc[Req, T], opts ...Option) Stage[Req, T] {
	s := newStage[Req, T](name, kindScoreEach, opts)
	s.score = fn
	return s
}

// ScoreBatch scores candidates with one call per batch (see WithMaxBatchSize).
// A wrong-length result fails the stage with ErrBatchSizeMismatch; a failed
// sub-batch fails the stage. A NaN score is an item error for that candidate
// and follows WithItemErrorPolicy. The scorer is not called on empty input.
func ScoreBatch[Req, T any](name string, fn BatchScoreFunc[Req, T], opts ...Option) Stage[Req, T] {
	s := newStage[Req, T](name, kindScoreBatch, opts)
	s.batch = fn
	return s
}

// TopK keeps the k highest-scoring candidates in descending score order.
// Equal scores preserve incoming order; +Inf is best; NaN follows WithNaN.
// k == 0 yields an empty slice; k >= len(in) sorts everything.
//
// TopK takes no callback, so its type arguments cannot be inferred:
// rankpipe.TopK[Req, T]("name", k). A local alias keeps pipelines tidy:
//
//	topK := rankpipe.TopK[Request, Doc]
func TopK[Req, T any](name string, k int, opts ...Option) Stage[Req, T] {
	s := newStage[Req, T](name, kindTopK, opts)
	s.k = k
	if k < 0 && s.err == nil {
		s.err = configErr("TopK(%q, %d): k must be >= 0", name, k)
	}
	return s
}

// Rerank applies a list-wise function that may reorder, remove and rescore
// candidates but may not add any: returning more candidates than received
// fails the stage with ErrRerankGrew. It is not called on empty input. Use it
// for diversity, freshness, deduplication, exploration, business constraints
// and list-wise models.
func Rerank[Req, T any](name string, fn StageFunc[Req, T], opts ...Option) Stage[Req, T] {
	s := newStage[Req, T](name, kindRerank, opts)
	s.list = fn
	return s
}

// Transform applies an unconstrained list-wise function: it may reorder,
// remove, add and rescore. It is always called, including on empty input.
// Prefer Rerank when the function should not add candidates. Transform is also
// how a hand-written Stage gets options: Transform(name, custom.Run, opts...).
func Transform[Req, T any](name string, fn StageFunc[Req, T], opts ...Option) Stage[Req, T] {
	s := newStage[Req, T](name, kindTransform, opts)
	s.list = fn
	return s
}

func (s *stage[Req, T]) Name() string { return s.name }

// Run executes the stage with its limiter and timeout applied. Error policies,
// fallbacks and reports are applied by the Pipeline, not here.
func (s *stage[Req, T]) Run(ctx context.Context, req Req, in []Candidate[T]) ([]Candidate[T], error) {
	out, _, err := s.exec(ctx, req, in)
	return out, err
}

func (s *stage[Req, T]) exec(ctx context.Context, req Req, in []Candidate[T]) ([]Candidate[T], stats, error) {
	if s.cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.timeout)
		defer cancel()
	}
	if l := s.cfg.limiter; l != nil {
		if err := l.Acquire(ctx); err != nil {
			return nil, stats{}, err
		}
		defer l.Release()
	}
	if err := ctx.Err(); err != nil {
		return nil, stats{}, err
	}
	switch s.kind {
	case kindFilter:
		return s.runFilter(ctx, req, in)
	case kindScoreEach:
		return s.runScoreEach(ctx, req, in)
	case kindScoreBatch:
		return s.runScoreBatch(ctx, req, in)
	case kindTopK:
		out, err := topK(in, s.k, s.cfg.nan)
		return out, stats{}, err
	case kindRerank:
		if len(in) == 0 {
			return in, stats{}, nil
		}
		out, err := s.list(ctx, req, in)
		if err != nil {
			return nil, stats{}, err
		}
		if len(out) > len(in) {
			return nil, stats{}, ErrRerankGrew
		}
		return out, stats{}, nil
	case kindTransform:
		out, err := s.list(ctx, req, in)
		return out, stats{}, err
	default:
		out, err := s.custom.Run(ctx, req, in)
		return out, stats{}, err
	}
}

// resolve validates option applicability and returns a private copy of the
// stage with its fallback chain resolved, so New never mutates a value the
// caller may still hold or share with another pipeline.
func (s *stage[Req, T]) resolve() (*stage[Req, T], error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.name == "" {
		return nil, configErr("stage has an empty name")
	}
	c := &s.cfg
	switch s.kind {
	case kindFilter:
		if s.filter == nil {
			return nil, configErr("Filter(%q): nil function", s.name)
		}
		if c.itemPolicy.kind == assignScore {
			return nil, configErr("Filter(%q): AssignScore is not a valid item policy for a filter", s.name)
		}
	case kindScoreEach:
		if s.score == nil {
			return nil, configErr("ScoreEach(%q): nil function", s.name)
		}
	case kindScoreBatch:
		if s.batch == nil {
			return nil, configErr("ScoreBatch(%q): nil function", s.name)
		}
	case kindRerank, kindTransform:
		if s.list == nil {
			return nil, configErr("%s(%q): nil function", s.kind.op(), s.name)
		}
	}
	if c.concurrency != 0 && s.kind != kindFilter && s.kind != kindScoreEach && s.kind != kindScoreBatch {
		return nil, configErr("stage %q: WithConcurrency does not apply to %s", s.name, s.kind.op())
	}
	if c.maxBatch != 0 && s.kind != kindScoreBatch {
		return nil, configErr("stage %q: WithMaxBatchSize only applies to ScoreBatch", s.name)
	}
	if c.itemPolicyOK && s.kind != kindFilter && s.kind != kindScoreEach && s.kind != kindScoreBatch {
		return nil, configErr("stage %q: WithItemErrorPolicy does not apply to %s", s.name, s.kind.op())
	}
	if c.nanSet && s.kind != kindTopK {
		return nil, configErr("stage %q: WithNaN only applies to TopK", s.name)
	}
	out := *s
	if c.fallback != nil {
		if c.policy != FailPipeline {
			return nil, configErr("stage %q: WithFallback and WithErrorPolicy(%v) are mutually exclusive", s.name, c.policy)
		}
		fb, ok := c.fallback.(Stage[Req, T])
		if !ok {
			return nil, configErr("stage %q: fallback stage has a different Req/T type", s.name)
		}
		resolved, err := asStage(fb).resolve()
		if err != nil {
			return nil, err
		}
		out.fallback = resolved
	}
	return &out, nil
}

// asStage returns the concrete stage behind a Stage, wrapping foreign
// implementations with default configuration.
func asStage[Req, T any](st Stage[Req, T]) *stage[Req, T] {
	if s, ok := st.(*stage[Req, T]); ok {
		return s
	}
	return &stage[Req, T]{name: st.Name(), kind: kindCustom, custom: st}
}

func formatFloat(f float64) string {
	if math.IsNaN(f) {
		return "NaN"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
