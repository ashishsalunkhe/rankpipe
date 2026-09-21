# rankpipe — architecture and API decisions

Status: v0.1 design. This document is normative for the implementation in this
repository; when code and this document disagree, one of them is a bug.

---

## 0. One-sentence thesis

`rankpipe` is **execution middleware for online ranking**: it owns the hard,
recurring *execution semantics* of a multi-stage ranking funnel (composition,
bounded concurrency, deadlines, candidate reduction, stable top-K, failure
policies, determinism, observability) and owns **nothing** about what a score
means or where candidates and models come from.

```
HTTP:      request → middleware → handler → response
rankpipe:  candidates → filter → score → reduce → expensive score → rerank → results
```

A Gorse-like recommender, a search service in front of OpenSearch, a dating
discovery service, or a matchmaking queue could each import it for their online
ranking path without `rankpipe` knowing which one it is embedded in.

---

## 1. What we studied and what we took

| System | What is valuable independent of the system | What we deliberately do not copy |
|---|---|---|
| **Vespa** phased ranking | (a) Cheap phase over *all* retrieved docs, expensive phases over a *strictly bounded* subset (`rerank-count`). (b) The bound is a first-class, explicit configuration knob, not an emergent property. (c) A "global" phase that sees the whole merged list for cross-candidate operations (normalisation, diversity). | Ranking expressions, feature language, content-node vs container split, ONNX evaluation. All of that is "what the score is", not "how the funnel executes". |
| **Elasticsearch** rescore | (a) `window_size`: expensive scoring is applied to a *window* of the previous phase's ordering. (b) Rescores *chain*: the second rescore sees the output of the first. (c) Score combination (`total`, `multiply`, …) is explicit; there is no implicit "the new score replaces the old". (d) A hard contract, not a hint: rescoring with a non-`_score` sort is an *error*. | The combination modes themselves. In `rankpipe` a scorer receives the candidate *with its current score*, so `0.3*prev + 0.7*model` is one line of user code and there is no combinator enum to grow. |
| **OpenSearch** search pipelines | (a) A linear, ordered list of processors is enough for a very large class of real systems. (b) Processors carry a `tag`/name used in errors and debug output. (c) `verbose_pipeline`: per-processor visibility of what went in and what came out is the primary debugging tool. (d) `keep_previous_score`: users *do* want previous scores, but only sometimes. | `ignore_failure`. "Ignore" conflates "continue with the input" with "pretend it succeeded" and makes degradation invisible. `rankpipe` names the policies (`ContinueWithInput`, fallback stage) and always reports them. |
| **LinkedIn PYMK** (L0/L1/L2/rerank) | Maintainability, monitoring and *coupling between stages* are the stated pain points of multi-stage rankers. The stage boundary must therefore be a real, typed, observable contract. | Nothing to copy — it is a description of a problem, and it is exactly the problem here. |
| **TensorFlow Ranking** | The clean separation between *model development/training* and *online application orchestration*. | Everything. `rankpipe` is on the other side of that line. |
| **Metarank** | Users of a ranking *service* want LTR, feature generation, personalisation, A/B testing and model serving in one box. That is the right product for teams who do not have those things. | The whole box. Teams that already have a feature store, a model server and an experiment system need a *library* that composes those, not a second service that duplicates them. |
| **Gorse** (Go) | A complete platform: master (training, config, membership), workers (offline per-user recommendations), servers (REST API, online serving), pluggable SQL/NoSQL stores, Redis cache. Its online path — "take cached offline recommendations, filter, fall back, return" — is precisely the shape of a `rankpipe` pipeline. | Nodes, storage, training, REST. `rankpipe` is what Gorse's *server* node could use inside its recommendation handler. It must never grow a process model. |
| **Feast** | Online feature retrieval is a *client call inside a stage* (typically a `ScoreBatch` stage fetching features for a batch, then scoring). | Feature definitions, materialisation, registry. `rankpipe` does not know what a feature is. |

### Go standard-library primitives: used vs not

| Primitive | Decision |
|---|---|
| `context` | Everything takes a `context.Context`; stage timeouts are `context.WithTimeout` children. Non-negotiable. |
| `errors` (`Is`/`As`/`Join`) | Structured `*Error` with `Unwrap`; sentinel errors; `errors.Join` only when a context error and a stage error must both be preserved. |
| `slices` | `SortStableFunc`/`SortFunc`/`Clone` — used. |
| `container/heap` | **Not used.** Its interface-based API boxes elements through `any` on every `Push`, and the pipeline's hot path is top-K. A 30-line typed sift-down on `[]ref` is faster and allocation-free. |
| `sync`, `sync/atomic` | `WaitGroup`, `Once`, `atomic.Int64` for the bounded worker engine. |
| `golang.org/x/sync/errgroup` | **Not used.** `errgroup.SetLimit` + `Go` per item would still create a closure per candidate; we need a *chunked index* engine, which is ~40 lines of `sync` + `atomic`. Not worth a dependency. |
| `golang.org/x/sync/semaphore` | **Not used.** The shared limiter is a buffered channel with a context-aware acquire (~30 lines). `semaphore.Weighted` adds a waiter list and weights we do not need. |
| `golang.org/x/sync/singleflight` | Not applicable in the core (request coalescing is an application concern). |

**v0.1 has zero non-stdlib dependencies.** `go.mod` declares only the module.

---

## 2. Decisions

Each decision states what was chosen, why, and what was rejected.

### D1. Pipeline model: linear and immutable in v0.1; DAG-ready internally

**Decision.** A pipeline is an ordered, immutable list of stages. Stage *i*'s
output slice is stage *i+1*'s input slice.

**Why.** Every reference system above (Vespa phases, ES rescore chains,
OpenSearch processors, LinkedIn L0→L1→L2) is linear at the level that matters.
A DAG needs: a scheduler, merge semantics for candidates arriving from
different branches (by identity? which `rankpipe` does not have), cancellation
of sibling branches on failure, a much larger error model, and an API that is
no longer a `[]Stage`. That is a v1.0 question at best.

**How the internals stay DAG-ready.** Composition happens exclusively through
the `Stage[Req, T]` interface; the runner is one loop that knows nothing about
what a stage does. A future `FanOut`/`Merge` is *a stage* that runs
sub-pipelines on copies of its input and merges — it needs no change to the
runner, the candidate type, the error type, or the report type. A sub-pipeline
is already usable as a stage via `Transform` (see §3).

### D2. Lifecycle: `New` validates and freezes; `Run` is safe for any number of goroutines

**Decision.** `New` performs *all* validation (names, option applicability,
mutually exclusive options) and returns an immutable `*Pipeline`. `Run` holds
no mutable pipeline state; all per-run state lives on the `Run` stack and in
the working slice.

**Why.** A pipeline is built once at service start and invoked from every
request goroutine. The only shared mutable thing a stage may hold is a
user-supplied `*Limiter`, which is itself concurrency-safe. Options are applied
by constructors into an unexported config and are not reachable after `New`, so
"configuration mutation after construction" cannot happen.

### D3. Generics: `Pipeline[Req, T]`

**Decision.** Two type parameters: the request/context type `Req` (query, user,
session — whatever the scorers need) and the candidate item type `T`.

**Why.** `any` in every callback pushes a type assertion into every stage a
user writes and hides mistakes until runtime. With generics the compiler checks
that every stage in a pipeline agrees on `Req` and `T`.

**Inference caveat (accepted).** Constructors that take a user function infer
`Req` and `T` from it. `TopK` takes no function, so it must be instantiated
explicitly: `rankpipe.TopK[Req, T]("k", 100)`. Go cannot infer type arguments
of a call from the parameter type of the enclosing call. The idiomatic
mitigation is one line per pipeline: `topK := rankpipe.TopK[Request, Profile]`.
A "builder" object (`b := rankpipe.For[Req,T]()`; `b.TopK(...)`) was rejected:
it introduces a second way to construct every stage for the benefit of one.

### D4. Candidate representation: one small value struct, one canonical score

```go
type Candidate[T any] struct {
    Item  T
    Score float64
}
```

**Decision.** `rankpipe` wraps each item in a 16-byte-plus-`T` value struct
with exactly one score. No ID, no metadata map, no score history, no rank
field, no hidden fields.

**Why each rejected alternative was rejected.**

- *`map[string]any` metadata / `map[string]float64` score history*: one
  allocation per candidate per stage that touches it; 50 000 candidates ×
  several stages is millions of allocations per request for a feature most
  consumers do not want. Users who want history put fields on `T` (§D16).
- *Required `ID()`/`Key()` on `T`*: not needed by any v0.1 primitive
  (TopK/Filter/Score do not need identity). Dedupe/Merge, which do, are
  post-v0.1 and can take a `func(T) K`.
- *Retaining rank/original index in the struct*: a hidden or exported index
  field costs 8 bytes per candidate and is *derivable*: a candidate's position
  in the slice **is** its rank at that point in the pipeline. Stability (§D8)
  is implemented with positions, not stored fields.
- *`*Candidate[T]` (pointer slices)*: worse locality, one allocation per
  candidate. A `[]Candidate[T]` of values is contiguous and copies in one
  `memmove`.
- *Making the current score private with accessors*: stages legitimately want
  to read the previous score and write the new one; a public field is the
  honest API.

**Guidance to users.** Keep `T` small: an ID, a pointer, or a small struct.
`rankpipe` moves `Candidate[T]` values when it compacts, sorts and copies; a
200-byte `T` makes every one of those 10× more expensive than a pointer.

### D5. Stage interface, constructors, options

```go
type Stage[Req, T any] interface {
    Name() string
    Run(ctx context.Context, req Req, in []Candidate[T]) ([]Candidate[T], error)
}
```

**Decision.** One small interface, consumed by the pipeline runner (which is
where behaviour is consumed, so the interface lives here). Built-in
constructors (`Filter`, `ScoreEach`, `ScoreBatch`, `TopK`, `Rerank`,
`Transform`) return `Stage[Req, T]` values whose concrete type carries an
unexported config. Anything implementing the interface is accepted by `New`.

**Options are non-generic** (`type Option func(*config) error`) so that
`WithConcurrency(16)` needs no type arguments. `WithFallback(stage)` is the one
generic option and infers its parameters from its argument. Applicability
(`WithConcurrency` on `TopK` is an error; `WithNaN` only on `TopK`) is checked
in `New`, not silently ignored — silent acceptance of a meaningless option is
how production misconfiguration hides.

**Policies split between stage and runner.** A stage's own `Run` applies the
things that are local to it: the shared limiter and the stage timeout. The
*runner* applies the things that need the pipeline: input snapshotting, error
policy, fallback, reporting. Consequence: calling a built-in stage's `Run`
directly gives you the bare operation plus its timeout/limiter; policies that
change what flows to the *next* stage only exist inside a pipeline. To attach
options to a hand-written `Stage`, wrap it: `Transform(name, custom.Run,
opts...)`.

### D6. Slice ownership contract

1. `Run(ctx, req, in)` **never modifies** `in` — neither the slice header nor
   the backing array. It copies `in` once into a working slice. The caller may
   reuse `in` immediately, including concurrently.
2. From that point the pipeline **owns the working slice**. Each stage receives
   it, may mutate it in place (write scores, compact, reorder) or return a new
   slice; whatever it returns is the next stage's input. Built-in stages exploit
   this: `Filter` compacts in place with zero allocations; `ScoreEach` writes
   scores in place.
3. `Result.Candidates` is owned by the caller and does not alias `in`.
4. Candidates are **shallow-copied**. If `T` is a pointer or contains
   references, every stage — and the caller's original slice — shares the
   pointee. `rankpipe` never deep-copies and never promises immutability of user
   objects; it cannot without knowing `T`.
5. `ScoreBatch` hands the batch scorer sub-slices with capacity clamped
   (`in[lo:hi:hi]`) so an accidental `append` cannot overwrite neighbours. The
   scorer must not reorder the slice it is given.

**Why copy on entry rather than take ownership?** Predictability. A single
`memmove` of N×sizeof(Candidate) is cheaper than the cheapest user filter over
the same N, and it removes an entire class of "my retrieval cache got sorted"
bugs. An ownership-transfer option can be added later without breaking
anything; the reverse is not true.

**Why a snapshot only under `ContinueWithInput`/fallback?** Those policies
promise "the input of the failed stage". A stage that failed halfway through
`ScoreEach` has already overwritten half the scores in place. So when — and
only when — a stage has a policy that can reuse its input, the runner clones
the input before running it. Placing such policies after a `TopK` keeps that
clone small; the docs say so.

### D7. Primitives in v0.1

Exactly six. Each has a *contract enforced by construction* (the primitive
cannot violate it) or *by check* (violations are stage errors).

| Primitive | May reorder | May remove | May add | May change scores | Enforcement |
|---|---|---|---|---|---|
| `Filter` | no | yes | no | no | construction |
| `ScoreEach` | no | only via `DropItem` policy | no | yes | construction |
| `ScoreBatch` | no | only via `DropItem` policy (NaN) | no | yes | construction |
| `TopK` | yes (by score) | yes | no | no | construction |
| `Rerank` | yes | yes | **no** | yes | `len(out) > len(in)` ⇒ `ErrRerankGrew` |
| `Transform` | yes | yes | yes | yes | none — the escape hatch |

`Rerank` and `Transform` take the same function type. They exist as two names
because *intent matters at review time*: a diversity reranker that starts
returning more candidates than it received is a bug, and `Rerank` catches it.
Rerankers cannot add, so `Rerank` is skipped on empty input; `Transform` is
always invoked (it may legitimately inject defaults).

Deferred (with the reason): `Conditional` (needs a predicate on `Req` — easy,
but wait for a real use case), `Fallback` as a *stage* (already covered by
`WithFallback`), `FanOut`/`Merge`/`Branch` (D1), `Dedupe` (needs a key
function; trivial to write as a `Rerank` today), `Retrieve` (a stage that
*creates* candidates from `Req` — belongs at the front, where `Run`'s input
already is), `Shadow` (run a stage and discard its output — a `Transform`
wrapper today).

### D8. TopK: algorithm, order, ties, NaN

**Order.** Higher score is better. Always. Not configurable per stage.
Rationale: a per-stage direction flag doubles the ways a pipeline can be
wrong (a `Descending` light ranker feeding an `Ascending` top-K sorts the worst
candidates to the top silently). Distances and penalties are negated by the
user's scorer — one character. If ascending ever becomes necessary it will be a
*pipeline-level* option so the whole funnel agrees.

**Ties.** Equal scores preserve incoming order. This is the only choice that
makes `TopK` a pure function of its input and lets upstream ordering (e.g.
retrieval rank) act as the implicit tiebreaker. `-0` and `+0` compare equal.
`+Inf` is the best possible score, `-Inf` the worst finite-or-not.

**NaN.** A NaN score is almost always an upstream bug (`0/0`, a model
returning NaN, a missing feature multiplied through). Policies:

- `NaNError` (**default**): the stage fails with `ErrNaNScore`.
- `NaNLowest`: NaN ranks below `-Inf`; ties among NaNs by position.
- `NaNDrop`: NaN candidates are removed before selection.

The default is the conservative one: silent corruption of an ordering is worse
than an error, and the error policy machinery (§D10) exists precisely to turn
that error into a fallback if the application prefers.

Scorers are checked too: a `ScoreEach`/`ScoreBatch` callback that returns NaN
is an **item-level error** (`ErrNaNScore`) and follows the item error policy,
so NaN rarely reaches `TopK` at all.

**Algorithm.** Let N = input length, K = requested.

- `K == 0` or `N == 0` → empty output, no user-visible work.
- `K < N` → a bounded min-heap of `(score, index)` references of size K, one
  pass over the input: O(N log K) time, 16·K bytes of scratch plus the
  K-element output. The comparator is a **total order** on `(score desc,
  index asc)`, so the final K-sort is deterministic without a stable sort.
  Because the heap holds references, large `T` is never moved during
  selection.
- `K >= N` → sort `(score, index)` references of every candidate with
  `slices.SortFunc` (pdqsort) under the same total order, then gather.

The original plan was an adaptive crossover to "stable-sort everything in
place" when K is a large fraction of N. Benchmarks killed it:
`slices.SortStableFunc` over 100 000 candidates takes ≈ 27 ms, the reference
pdqsort ≈ 13 ms, and the heap is faster or equal at *every* K < N (≈ 0.13 ms
at K = 10, ≈ 12.6 ms at K = 75 000). So there is no crossover constant to
tune: heap below N, reference sort at N. `BenchmarkTopK` keeps both strategies
measurable so the decision can be re-checked on other hardware.

The heap is a hand-written sift-down on a typed slice rather than
`container/heap` (see §1).

**Verification.** Property tests compare both paths against a reference
implementation (stable sort + truncate) over random N, K, heavy ties, ±Inf,
±0 and NaN under each policy.

### D9. Deadlines and cancellation

- The runner checks `ctx.Err()` before every stage. A request that is already
  cancelled before `Run` fails before any user code runs.
- `WithTimeout(d)` derives a child context for that stage. If the parent's
  remaining budget is shorter, the child inherits the shorter deadline —
  `context` already does this; there is nothing to configure.
- **All user code runs synchronously on the caller's goroutine or on the
  stage's bounded workers.** `rankpipe` never launches user code and returns
  while it continues. A stage that ignores its context cannot be preempted;
  the timeout then takes effect at the next point where `rankpipe` observes
  the context (between items, between chunks, between stages). This is
  documented as *cooperative cancellation* and is not "solved" with detached
  goroutines because that leaks work.
- Inside `ScoreEach`/`Filter`/`ScoreBatch` with concurrency > 1, the first
  error cancels a stage-scoped context; other workers stop at their next item
  boundary; the stage waits for all workers before returning. No goroutine
  outlives the stage call.
- **Cancellation is never an item-level error.** If a scorer returns an error
  and the stage context is done, the stage fails with the context error
  regardless of the item error policy.
- **Parent vs stage deadline decides which policy applies.** If a stage fails
  and the *parent* context is done, the pipeline fails: nothing downstream can
  run anyway, and `errors.Is(err, context.DeadlineExceeded)` holds. If only the
  *stage* deadline fired, the stage's error policy applies — that is the whole
  point of a stage timeout: "give the deep ranker 40 ms, otherwise fall back".
- Latency *budget allocation* across stages (e.g. "give the remainder to the
  reranker") is explicitly post-v0.1. v0.1 gives you the parent deadline and
  per-stage caps, which is what Vespa and ES expose too.

### D10. Failure semantics

Two independent axes, never sharing a policy value.

**Stage-level** (`WithErrorPolicy`, `WithFallback`):

| Policy | Behaviour | Visible as |
|---|---|---|
| `FailPipeline` (default) | `Run` returns `*Error`; `Result.Stages` holds reports up to the failure. | `StageReport.Status == StatusFailed` |
| `ContinueWithInput` | The failed stage's *input* (snapshotted, §D6) is passed to the next stage. | `StatusDegraded`, `Result.Degraded = true` |
| `WithFallback(stage)` | The fallback stage runs on the failed stage's input; its output flows on. The fallback has its own options (timeout, policy…). | `StatusFellBack` + a second report with `Fallback: true` |

`ReturnPartial` (stop and return whatever we have) was considered and rejected
for v0.1: the input to a failed deep ranker is 1 000 candidates on the light
ranker's score scale, not 20 final results; returning it "as the result"
is rarely what a caller wants. `ContinueWithInput` dominates it: downstream
`TopK` and reranking still run and still shape the output. `ContinueWithInput`
and `WithFallback` are mutually exclusive (compile-time-ish: `New` rejects
both).

**Item-level** (`WithItemErrorPolicy`, applies to `Filter`, `ScoreEach`, and to
NaN scores from `ScoreBatch`):

| Policy | `ScoreEach` | `Filter` |
|---|---|---|
| `FailStage` (default) | first item error fails the stage (with `ItemIndex`) | same |
| `DropItem` | candidate removed, order preserved | candidate removed |
| `KeepItem` | previous score retained | candidate kept |
| `AssignScore(v)` | score set to `v` | *invalid* (rejected by `New`) |

Defaults are conservative on both axes. Every tolerated item error is counted
in `StageReport.ItemErrors` and sets `Result.Degraded`. "Silent candidate
corruption is worse than an error" is the tie-breaker for every default.

**Sub-batch failure in `ScoreBatch`** is a *stage* error, not an item error:
losing a whole batch of scores is not a per-candidate event, and the
stage-level policies (fallback to the light ordering) are the right tools.

### D11. Determinism

Given the same request, input, configuration and user-function outputs, the
output is byte-for-byte identical, including under concurrency:

- Parallel scoring writes `in[i].Score` **by index**; nothing about the order
  in which workers finish affects the slice.
- Filtering with concurrency records a keep-bitmap by index and compacts
  sequentially.
- `TopK` uses a total order (§D8).
- No map iteration anywhere on the data path.
- The one intentionally nondeterministic detail: *which* item error is
  reported first when several occur concurrently under `FailStage`. The
  documentation says so.

Tests run the same concurrent pipeline hundreds of times and from many
goroutines under `-race` and require identical output.

### D12. Concurrency within a request

- `WithConcurrency(n)`: `n <= 1` is sequential (the default). `n > 1` runs up
  to `min(n, N)` workers over the candidate index space.
- **No goroutine per candidate.** Workers claim *chunks* of indices with a
  single atomic add; chunk size is 1 for small inputs (so a slow RPC on the
  last item does not leave workers idle) and grows for large inputs so that
  100 000 cheap items do not pay 100 000 contended atomics.
- The calling goroutine is one of the workers.
- Sequential loops check the context every 256 items (a context `Err()` call
  takes a mutex; per-item checks are measurable on 100k cheap items) — and
  always on the item-error path.
- **Parallelism is not free and the docs say so.** For a scorer that costs
  tens of nanoseconds, the sequential path wins; the benchmarks in this
  repository demonstrate it. Concurrency is for I/O-bound per-item work.

### D13. Concurrency across requests and backpressure

A pipeline with `WithConcurrency(16)` on a stage, invoked by 10 000 concurrent
requests, would want 160 000 goroutines. `rankpipe` cannot own admission
control, but it must not make the situation worse than "the application has
10 000 requests". So v0.1 ships one deliberately small primitive:

```go
lim := rankpipe.NewLimiter(64)                 // shared by all requests
rankpipe.ScoreBatch("deep", model, rankpipe.WithLimiter(lim), rankpipe.WithTimeout(40*time.Millisecond))
```

- One permit is held for the duration of one *stage invocation* (not one
  worker), so total in-flight work in that stage is `limit × concurrency`.
- Acquisition is context-aware and happens **after** the stage timeout is
  applied, so a request that cannot start the deep ranker within its budget
  fails that stage (and falls back, if configured) instead of queueing
  forever. There is no hidden queue: waiters are the request goroutines that
  already exist, and they leave when their context ends.
- Anything more (priorities, weighted permits, adaptive concurrency) belongs to
  the application or a dedicated library; the `Limiter` is a plain struct so it
  can be replaced later by an interface if a second implementation appears.

### D14. Batch scoring edge cases

| Case | Behaviour |
|---|---|
| empty input | scorer not called |
| `len(scores) != len(batch)` | stage error `ErrBatchSizeMismatch` (never item-level; positional corruption is unacceptable) |
| partial response | same as above — an adapter that can only score some items must return a full-length slice and choose NaN (item policy applies) or a sentinel |
| NaN in a score | item-level `ErrNaNScore`, item policy applies |
| `WithMaxBatchSize(m)` | input split into ⌈N/m⌉ sub-batches of ≤ m, each scored by one call; with `WithConcurrency(c)` up to `c` sub-batches run in parallel |
| one sub-batch fails | stage fails (§D10); no partial commit is attempted |
| RPC timeout / cancellation | stage error with the context cause; policy per §D9 |
| server returns different order / duplicate IDs | out of scope by design — `rankpipe` has no identity. The user's adapter maps responses back by its own key; the docs show the pattern |
| cross-request micro-batching | out of scope for v0.1 and probably forever as a *core* feature. The `BatchScoreFunc` signature is exactly what a micro-batcher exposes as its client call, so one can be plugged in |

### D15. Score semantics

Scores are opaque `float64` values with a single agreed meaning: **larger is
better**. `rankpipe` never normalises, combines, or interprets them. A
`ScoreEach`/`ScoreBatch` callback receives the candidate *including its current
score*, so combination is user code and needs no enum:

```go
func(ctx, req, c Candidate[Doc]) (float64, error) { return 0.3*c.Score + 0.7*model(...), nil }
```

IEEE-754: `+Inf` best, `-Inf` worst, `±0` equal, NaN per §D8/§D10.

### D16. Score history

Not in the core. The options considered:

1. `map[string]float64` on every candidate — rejected (§D4).
2. A fixed `[N]float64` ring on every candidate — arbitrary N, still 8N bytes
   per candidate for everyone.
3. Fields on the user's `T` — free, typed, and exactly what a user who cares
   about history wants (`profile.LightScore = c.Score` in a `Transform`).
4. A per-run observer callback receiving the slice after each stage — enables
   history, logging and metrics, but needs pipeline-level configuration and
   invites callers to retain slices they do not own.

v0.1 chooses **3** as the recommendation and provides **per-stage reports**
(§D17) for the non-per-candidate part. A read-only observer (4) is the natural
v0.2 addition if demand is real; nothing in v0.1 precludes it.

### D17. Observability

```go
res, err := p.Run(ctx, req, cands)
res.Candidates  // final list (nil on error)
res.Degraded    // any non-default policy was exercised
res.Stages      // one StageReport per executed stage: name, index, in/out counts, duration, status, item errors, error
```

`Run` returns a `Result[T]` rather than a bare slice because degradation must
be visible *at the call site*, not only in logs. The report slice is one small
allocation per run (≈ 80 bytes × stages), negligible next to the working
slice. On error, `Result.Stages` still contains the reports up to and including
the failure — that is the "verbose pipeline" you want when paged. There is no
callback interface in v0.1; applications emit metrics from `res.Stages` after
`Run`, synchronously, with no risk of retaining pipeline-owned slices.

### D18. Errors

```go
type Error struct {
    Stage     string // stage name
    Index     int    // stage index in the pipeline (-1 if none)
    Op        string // "filter", "score", "score-batch", "top-k", "rerank", "transform", "custom", "run"
    ItemIndex int    // index of the offending candidate, or -1
    Err       error  // cause
}
```

`Unwrap` returns `Err`, so `errors.Is(err, context.DeadlineExceeded)`,
`errors.Is(err, rankpipe.ErrNaNScore)` and `errors.As(err, &e)` all work. When
the parent context is done and the stage returned a *different* error, both are
kept via `errors.Join` so neither is lost. Sentinels: `ErrInvalidConfig`,
`ErrNaNScore`, `ErrBatchSizeMismatch`, `ErrRerankGrew`.

No pipeline name field: the application knows which pipeline it called, and
the stage name + index pin the location. Adding `Pipeline string` later is a
non-breaking change.

---

## 3. API surface (v0.1)

```go
// Types
type Candidate[T any] struct { Item T; Score float64 }
type Stage[Req, T any] interface { Name() string; Run(ctx, Req, []Candidate[T]) ([]Candidate[T], error) }
type Pipeline[Req, T any] struct{ /* immutable */ }
type Result[T any] struct { Candidates []Candidate[T]; Degraded bool; Stages []StageReport }
type StageReport struct { Name string; Index int; Fallback bool; In, Out int; Duration time.Duration; Status Status; ItemErrors int; Err error }
type Error struct { Stage string; Index int; Op string; ItemIndex int; Err error }
type Limiter struct{ /* … */ }

// Callbacks
type FilterFunc[Req, T any]     func(context.Context, Req, Candidate[T]) (bool, error)
type ScoreFunc[Req, T any]      func(context.Context, Req, Candidate[T]) (float64, error)
type BatchScoreFunc[Req, T any] func(context.Context, Req, []Candidate[T]) ([]float64, error)
type StageFunc[Req, T any]      func(context.Context, Req, []Candidate[T]) ([]Candidate[T], error)

// Construction
func New[Req, T any](stages ...Stage[Req, T]) (*Pipeline[Req, T], error)
func Filter[Req, T any](name string, fn FilterFunc[Req, T], opts ...Option) Stage[Req, T]
func ScoreEach[Req, T any](name string, fn ScoreFunc[Req, T], opts ...Option) Stage[Req, T]
func ScoreBatch[Req, T any](name string, fn BatchScoreFunc[Req, T], opts ...Option) Stage[Req, T]
func TopK[Req, T any](name string, k int, opts ...Option) Stage[Req, T]
func Rerank[Req, T any](name string, fn StageFunc[Req, T], opts ...Option) Stage[Req, T]
func Transform[Req, T any](name string, fn StageFunc[Req, T], opts ...Option) Stage[Req, T]

// Options (validated in New)
func WithConcurrency(n int) Option                    // Filter, ScoreEach, ScoreBatch
func WithMaxBatchSize(n int) Option                   // ScoreBatch
func WithItemErrorPolicy(p ItemErrorPolicy) Option    // Filter, ScoreEach, ScoreBatch
func WithNaN(p NaNPolicy) Option                      // TopK
func WithTimeout(d time.Duration) Option              // any
func WithErrorPolicy(p ErrorPolicy) Option            // any
func WithFallback[Req, T any](s Stage[Req, T]) Option // any
func WithLimiter(l *Limiter) Option                   // any

// Run
func (p *Pipeline[Req, T]) Run(ctx context.Context, req Req, in []Candidate[T]) (Result[T], error)

// Helpers
func Candidates[T any](items []T) []Candidate[T]
func Items[T any](cs []Candidate[T]) []T
func NewLimiter(n int) *Limiter
```

Critique of the illustrative API in the brief, and what changed:

- `Run` returns `Result[T]`, not `[]Candidate[T]`: degradation must be visible.
- `Filter`'s callback takes `Candidate[T]`, not `T`: score-threshold filters
  are common and need the score; consistency across all per-item callbacks.
- `TopK` needs explicit type arguments (D3). Everything else infers.
- `Rerank` is constrained (cannot grow); `Transform` is the unconstrained one.
- Stage timeouts, error policies, fallbacks and limiters are *options on any
  stage*, not separate wrapper stages, so a pipeline reads top-to-bottom as its
  funnel and every stage's operational contract is next to its name.

---

## 4. Explicitly out of scope for v0.1 (and why)

- **DAG / branching / merging** — D1.
- **Latency budget allocator** — D9; per-stage caps + parent deadline first.
- **Cross-request micro-batching** — D14; a separate component that plugs into
  `BatchScoreFunc`.
- **Observer/tracing callback and per-candidate score history** — D16/D17;
  per-stage reports first, callback if demanded.
- **Ascending order** — D8; would be pipeline-level, not per stage.
- **Retries** — a retry is a policy on a *remote call*, which lives inside the
  user's scorer where it knows what is idempotent. Retrying a whole stage
  wholesale is rarely right and easy to write as a `Transform` wrapper.
- **Anything domain-specific** (IDs, features, models, diversity algorithms).
