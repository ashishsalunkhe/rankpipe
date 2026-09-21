// Package rankpipe is execution middleware for online ranking.
//
// Large recommendation, search, feed, marketplace and matchmaking systems rank
// candidates in a funnel: cheap filters and scorers over many candidates, a
// bounded top-K, expensive scorers over the survivors, another top-K, then
// list-wise reranking. rankpipe owns the execution semantics of that funnel —
// composition, bounded concurrency, deadlines, candidate reduction, stable
// top-K, failure policies, determinism and per-stage reporting — and nothing
// about what a score means or where candidates, features or models come from.
//
//	pipeline, err := rankpipe.New(
//	    rankpipe.Filter("eligibility", eligible),
//	    rankpipe.ScoreEach("light", lightScore, rankpipe.WithConcurrency(8)),
//	    rankpipe.TopK[Request, Doc]("light-top-k", 1000),
//	    rankpipe.ScoreBatch("deep", deepModel,
//	        rankpipe.WithMaxBatchSize(256),
//	        rankpipe.WithTimeout(40*time.Millisecond),
//	        rankpipe.WithFallback(rankpipe.Transform("keep-light", identity))),
//	    rankpipe.TopK[Request, Doc]("deep-top-k", 100),
//	    rankpipe.Rerank("diversity", diversify),
//	    rankpipe.TopK[Request, Doc]("final", 20),
//	)
//	res, err := pipeline.Run(ctx, req, rankpipe.Candidates(docs))
//
// # Contracts
//
// Scores are opaque float64 values; higher is better. Equal scores preserve
// incoming order. Run never modifies the caller's input slice; it copies it
// once and the pipeline owns the working slice from then on. Candidates are
// shallow-copied: if T is a pointer, every stage shares the pointee.
//
// All user callbacks run synchronously on the calling goroutine or on the
// stage's bounded workers; rankpipe never detaches user code. Cancellation is
// cooperative: a callback that ignores its context cannot be preempted, and a
// stage timeout takes effect at the next point rankpipe observes the context.
//
// A built Pipeline is immutable and safe for concurrent use by any number of
// goroutines.
//
// See DESIGN.md in the repository for the full rationale behind every decision.
package rankpipe
