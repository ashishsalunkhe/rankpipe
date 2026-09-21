package rankpipe_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ashishsalunkhe/rankpipe"
)

// Domain types live in the application; rankpipe never sees their shape.
type Request struct {
	UserID   string
	Blocked  map[string]bool
	Interest map[string]float64
}

type Profile struct {
	ID     string
	Topics []string
	AgeDay int // days since profile creation
}

func eligible(_ context.Context, req Request, c rankpipe.Candidate[*Profile]) (bool, error) {
	return !req.Blocked[c.Item.ID], nil
}

func lightScore(_ context.Context, req Request, c rankpipe.Candidate[*Profile]) (float64, error) {
	s := 0.0
	for _, t := range c.Item.Topics {
		s += req.Interest[t]
	}
	return s, nil
}

// deepModel stands in for a remote model call scoring a whole batch. It
// receives the candidates with their light score, so it can combine.
func deepModel(ctx context.Context, req Request, in []rankpipe.Candidate[*Profile]) ([]float64, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	out := make([]float64, len(in))
	for i, c := range in {
		out[i] = 0.3*c.Score + 0.7*math.Tanh(float64(len(c.Item.Topics)))
	}
	return out, nil
}

// freshness boosts new profiles; it may reorder and rescore but not add.
func freshness(_ context.Context, _ Request, in []rankpipe.Candidate[*Profile]) ([]rankpipe.Candidate[*Profile], error) {
	for i := range in {
		if in[i].Item.AgeDay < 7 {
			in[i].Score += 0.1
		}
	}
	// Reordering after a rescore is the reranker's job.
	out, err := rankpipe.TopK[Request, *Profile]("sort", len(in)).Run(context.Background(), Request{}, in)
	return out, err
}

func Example() {
	topK := rankpipe.TopK[Request, *Profile]
	keepLight := rankpipe.Transform("keep-light-scores",
		func(_ context.Context, _ Request, in []rankpipe.Candidate[*Profile]) ([]rankpipe.Candidate[*Profile], error) {
			return in, nil
		})

	pipeline, err := rankpipe.New(
		rankpipe.Filter("eligibility", eligible),
		rankpipe.ScoreEach("light-ranker", lightScore, rankpipe.WithConcurrency(8)),
		topK("light-top-k", 1000),
		rankpipe.ScoreBatch("deep-ranker", deepModel,
			rankpipe.WithMaxBatchSize(256),
			rankpipe.WithTimeout(40*time.Millisecond),
			rankpipe.WithFallback(keepLight), // model down → serve the light ordering
		),
		topK("deep-top-k", 100),
		rankpipe.Rerank("freshness", freshness),
		topK("final", 3),
	)
	if err != nil {
		panic(err)
	}

	req := Request{
		UserID:   "u1",
		Blocked:  map[string]bool{"p2": true},
		Interest: map[string]float64{"hiking": 1, "jazz": 0.5},
	}
	profiles := []*Profile{
		{ID: "p1", Topics: []string{"hiking"}, AgeDay: 100},
		{ID: "p2", Topics: []string{"hiking", "jazz"}, AgeDay: 1},
		{ID: "p3", Topics: []string{"jazz"}, AgeDay: 2},
		{ID: "p4", Topics: []string{"hiking", "jazz"}, AgeDay: 30},
		{ID: "p5", Topics: nil, AgeDay: 3},
	}

	res, err := pipeline.Run(context.Background(), req, rankpipe.Candidates(profiles))
	if err != nil {
		var e *rankpipe.Error
		if errors.As(err, &e) {
			fmt.Println("failed at stage", e.Stage, "op", e.Op)
		}
		return
	}
	for _, c := range res.Candidates {
		fmt.Printf("%s %.3f\n", c.Item.ID, c.Score)
	}
	fmt.Println("degraded:", res.Degraded)
	for _, r := range res.Stages {
		fmt.Printf("%-18s %3d -> %3d %s\n", r.Name, r.In, r.Out, r.Status)
	}
	// Output:
	// p4 1.125
	// p1 0.833
	// p3 0.783
	// degraded: false
	// eligibility          5 ->   4 ok
	// light-ranker         4 ->   4 ok
	// light-top-k          4 ->   4 ok
	// deep-ranker          4 ->   4 ok
	// deep-top-k           4 ->   4 ok
	// freshness            4 ->   4 ok
	// final                4 ->   3 ok
}

// ExampleWithFallback shows degradation being visible at the call site.
func ExampleWithFallback() {
	down := func(context.Context, string, []rankpipe.Candidate[int]) ([]float64, error) {
		return nil, errors.New("model service unavailable")
	}
	light := func(_ context.Context, _ string, in []rankpipe.Candidate[int]) ([]rankpipe.Candidate[int], error) {
		return in, nil // keep the scores already on the candidates
	}
	p, _ := rankpipe.New(
		rankpipe.ScoreBatch("deep", down, rankpipe.WithFallback(rankpipe.Transform("light", light))),
		rankpipe.TopK[string, int]("final", 2),
	)
	in := []rankpipe.Candidate[int]{{Item: 1, Score: 0.2}, {Item: 2, Score: 0.9}, {Item: 3, Score: 0.5}}
	res, err := p.Run(context.Background(), "user", in)
	fmt.Println(err, res.Degraded, rankpipe.Items(res.Candidates))
	for _, r := range res.Stages {
		fmt.Println(r.Name, r.Status, r.Fallback)
	}
	// Output:
	// <nil> true [2 3]
	// deep fell-back false
	// light ok true
	// final ok false
}

// ExampleWithItemErrorPolicy shows per-candidate failures being tolerated and counted.
func ExampleWithItemErrorPolicy() {
	score := func(_ context.Context, _ struct{}, c rankpipe.Candidate[string]) (float64, error) {
		if c.Item == "b" {
			return 0, errors.New("feature missing")
		}
		return float64(len(c.Item)), nil
	}
	p, _ := rankpipe.New(
		rankpipe.ScoreEach("score", score, rankpipe.WithItemErrorPolicy(rankpipe.DropItem)),
	)
	res, _ := p.Run(context.Background(), struct{}{}, rankpipe.Candidates([]string{"a", "b", "ccc"}))
	fmt.Println(rankpipe.Items(res.Candidates), res.Stages[0].ItemErrors, res.Degraded)
	// Output:
	// [a ccc] 1 true
}
