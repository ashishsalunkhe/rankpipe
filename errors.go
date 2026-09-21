package rankpipe

import (
	"errors"
	"fmt"
)

// Sentinel errors. Match them with errors.Is; they are always wrapped in an
// *Error when returned from Run.
var (
	// ErrInvalidConfig is returned by New when a stage or option is misconfigured.
	ErrInvalidConfig = errors.New("rankpipe: invalid configuration")

	// ErrNaNScore reports a NaN score: from a scorer (item-level, subject to the
	// item error policy) or reaching TopK under NaNError (stage-level).
	ErrNaNScore = errors.New("rankpipe: NaN score")

	// ErrBatchSizeMismatch reports a batch scorer returning a slice whose length
	// differs from its input. This is always a stage-level failure: positional
	// scores cannot be trusted after a length mismatch.
	ErrBatchSizeMismatch = errors.New("rankpipe: batch scorer returned wrong number of scores")

	// ErrRerankGrew reports a Rerank function returning more candidates than it
	// received. Rerankers may reorder, remove and rescore, never add; use
	// Transform for stages that add candidates.
	ErrRerankGrew = errors.New("rankpipe: rerank returned more candidates than it received")
)

// Operation names carried in Error.Op.
const (
	OpRun        = "run"
	OpFilter     = "filter"
	OpScore      = "score"
	OpScoreBatch = "score-batch"
	OpTopK       = "top-k"
	OpRerank     = "rerank"
	OpTransform  = "transform"
	OpCustom     = "custom"
)

// Error is the error type returned by Pipeline.Run. It locates the failure
// (stage name, index, operation, optionally the offending candidate) and wraps
// the cause so errors.Is and errors.As see through it, including for
// context.Canceled and context.DeadlineExceeded.
type Error struct {
	Stage     string // name of the stage that failed ("" for pipeline-level failures)
	Index     int    // index of the stage in the pipeline, or -1
	Op        string // one of the Op* constants
	ItemIndex int    // index of the offending candidate in the stage input, or -1
	Err       error  // cause
}

func (e *Error) Error() string {
	var where string
	switch {
	case e.Stage == "":
		where = "pipeline"
	case e.ItemIndex >= 0:
		where = fmt.Sprintf("stage %q (index %d, %s) item %d", e.Stage, e.Index, e.Op, e.ItemIndex)
	default:
		where = fmt.Sprintf("stage %q (index %d, %s)", e.Stage, e.Index, e.Op)
	}
	return "rankpipe: " + where + ": " + e.Err.Error()
}

// Unwrap returns the cause.
func (e *Error) Unwrap() error { return e.Err }

// itemError is produced inside a stage when a per-candidate failure escalates
// to a stage failure under FailStage. The runner unpacks it into Error.ItemIndex.
type itemError struct {
	index int
	err   error
}

func (e *itemError) Error() string { return fmt.Sprintf("item %d: %v", e.index, e.err) }
func (e *itemError) Unwrap() error { return e.err }

func configErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidConfig, fmt.Sprintf(format, args...))
}
