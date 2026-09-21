# rankpipe

Execution middleware for online ranking, as a small, dependency-free Go
package.

```
candidates → filter → score → top-K → expensive score → top-K → rerank → results
```

Recommendation, search, feed, marketplace, matchmaking and ads services all
rank candidates in a funnel: cheap filters and scorers over many candidates, a
bounded top-K, expensive scorers over the survivors, list-wise reranking.
`rankpipe` owns the *execution semantics* of that funnel — composition,
bounded concurrency, deadlines, stable top-K, failure policies, determinism
and per-stage reporting — and knows nothing about what a score means or where
candidates, features and models come from. Your retrieval system, feature
store and model server plug in as ordinary Go functions.

It is the ranking equivalent of an HTTP router: it does not implement your
handlers, it runs them well.

## Install

```bash
go get github.com/ashishsalunkhe/rankpipe
```

Go 1.22+. No dependencies outside the standard library.

## Example

```go
topK := rankpipe.TopK[Request, *Profile] // TopK takes no callback, so name its types once

pipeline, err := rankpipe.New(
    rankpipe.Filter("eligibility", eligible),
    rankpipe.ScoreEach("light-ranker", lightScore, rankpipe.WithConcurrency(8)),
    topK("light-top-k", 1000),
    rankpipe.ScoreBatch("deep-ranker", deepModel,
        rankpipe.WithMaxBatchSize(256),
        rankpipe.WithTimeout(40*time.Millisecond),
        rankpipe.WithFallback(keepLightScores), // model down → serve the light ordering
    ),
    topK("deep-top-k", 100),
    rankpipe.Rerank("freshness", freshness),
    topK("final", 20),
)

res, err := pipeline.Run(ctx, req, rankpipe.Candidates(profiles))
if err != nil { /* *rankpipe.Error: stage, index, op, cause */ }

res.Candidates // []rankpipe.Candidate[*Profile], best first
res.Degraded   // true if a fallback ran, a stage continued with its input, or item errors were tolerated
res.Stages     // one report per stage: in/out counts, duration, status, item errors, error
```

where the user functions have these shapes:

```go
func eligible(ctx context.Context, req Request, c rankpipe.Candidate[*Profile]) (bool, error)
func lightScore(ctx context.Context, req Request, c rankpipe.Candidate[*Profile]) (float64, error)
func deepModel(ctx context.Context, req Request, in []rankpipe.Candidate[*Profile]) ([]float64, error)
func freshness(ctx context.Context, req Request, in []rankpipe.Candidate[*Profile]) ([]rankpipe.Candidate[*Profile], error)
```

See [`example_test.go`](example_test.go) for a complete, runnable version.

## The six primitives

| Stage | Callback | Contract |
|---|---|---|
| `Filter` | per candidate → `bool` | removes; never reorders |
| `ScoreEach` | per candidate → `float64` | rescores in place; order preserved even with `WithConcurrency` |
| `ScoreBatch` | per batch → `[]float64` | one call per batch (`WithMaxBatchSize`); wrong length is a stage error |
| `TopK` | – | keeps the best K, descending; equal scores keep input order |
| `Rerank` | whole list → list | may reorder, remove, rescore; **may not add** |
| `Transform` | whole list → list | unconstrained escape hatch; also how a custom `Stage` gets options |

Scorers receive the candidate *with its current score*, so combining the
previous score with a new one (`0.3*c.Score + 0.7*model(...)`) is ordinary user
code — there is no combinator enum.

## Options

| Option | Applies to | Meaning |
|---|---|---|
| `WithConcurrency(n)` | Filter, ScoreEach, ScoreBatch | up to `n` workers claim chunks of candidates (never a goroutine per candidate) |
| `WithMaxBatchSize(n)` | ScoreBatch | split into sub-batches of ≤ n; parallel with `WithConcurrency` |
| `WithItemErrorPolicy(p)` | Filter, ScoreEach, ScoreBatch | `FailStage` (default), `DropItem`, `KeepItem`, `AssignScore(v)` |
| `WithNaN(p)` | TopK | `NaNError` (default), `NaNLowest`, `NaNDrop` |
| `WithTimeout(d)` | any | child-context deadline for the stage |
| `WithErrorPolicy(p)` | any | `FailPipeline` (default) or `ContinueWithInput` |
| `WithFallback(stage)` | any | run another stage on the failed stage's input |
| `WithLimiter(l)` | any | shared cap on concurrent invocations of this stage across all requests |

Options that do not apply to a stage are rejected by `New`, not ignored.

## Semantics you can rely on

**Ownership.** `Run` never modifies your input slice; it copies it once. Every
stage then owns the working slice and may mutate it in place. The result does
not alias your input. Candidates are shallow copies: if `T` is a pointer,
every stage shares the pointee.

**Order.** Higher score is better. Equal scores keep incoming order. `+Inf` is
best; `-0 == +0`; NaN from a scorer is an item-level error (`ErrNaNScore`)
and NaN at `TopK` follows `WithNaN`, defaulting to an error.

**Determinism.** Same request, input, configuration and callback outputs ⇒
identical output, including under `WithConcurrency`. Parallel scoring writes
by index; nothing depends on scheduling.

**Deadlines.** The parent context is checked before every stage. A stage
timeout derives a child context. If only the *stage* deadline fires, the
stage's error policy applies (that is what a stage timeout is for: "give the
deep ranker 40 ms, otherwise fall back"). If the *parent* context is done, the
pipeline fails regardless of policy, and `errors.Is(err, context.DeadlineExceeded)`
holds. Cancellation is cooperative: `rankpipe` never runs your code on a
detached goroutine, so a callback that ignores its context cannot be
preempted.

**Failure.** Stage-level and item-level failures have separate policies and
conservative defaults; nothing is silently ignored. Every tolerated failure is
counted in `res.Stages` and sets `res.Degraded`. Errors are `*rankpipe.Error`
values carrying stage name, index, operation and (for item failures) the
candidate index, and unwrap to their cause.

**Concurrency across requests.** A built `Pipeline` is immutable and safe for
any number of concurrent `Run` calls. A stage with `WithConcurrency(16)` under
10 000 concurrent requests would want 160 000 goroutines; bound it with a
shared `Limiter`:

```go
deepLimit := rankpipe.NewLimiter(64)
rankpipe.ScoreBatch("deep", model, rankpipe.WithLimiter(deepLimit), rankpipe.WithTimeout(40*time.Millisecond))
```

Acquisition honours the stage-timeout-bounded context, so a request that
cannot start the deep ranker in time fails that stage (and falls back if
configured) instead of queueing.

## Performance notes

From `go test -bench .` on an Apple M-series laptop (see `bench_test.go`):

- The 50 000 → 20 funnel above with in-process callbacks costs ≈ 0.6 ms and
  19 allocations of framework overhead per request.
- `TopK` uses a bounded heap of `(score, index)` references for K < N
  (100 000 → 100 in ≈ 0.15 ms, two allocations) and a pdqsort over references
  when everything must be ordered.
- Concurrency is not free. A ~5 ns scorer over 100 000 candidates runs in
  0.56 ms sequentially, 0.29 ms with 4 workers, and *0.38 ms* with 16. A 50 µs
  RPC-like scorer over 1 000 candidates goes from 68 ms to 4.3 ms with 16
  workers. Use `WithConcurrency` for I/O-bound callbacks; benchmark the rest.

## What rankpipe is not

Not a recommender, a vector database, a feature store, a search engine, a
model server, a training framework, an experiment platform or a workflow
orchestrator. All of those sit *underneath* a `rankpipe` stage, called from
your code. See [DESIGN.md](DESIGN.md) for the full rationale, the systems
studied (Vespa, Elasticsearch, OpenSearch, Metarank, Gorse, Feast, TF-Ranking)
and every design decision with its rejected alternatives.

## Status

v0.1 — API may change before v1.0. Deliberately out of scope for now:
DAG/branching pipelines, latency budget allocation across stages,
cross-request micro-batching, per-candidate score history, ascending order.
DESIGN.md §4 explains each.

## License

MIT.
