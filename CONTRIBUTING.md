# Contributing to rankpipe

Thanks for your interest. rankpipe is deliberately small; the bar for adding
surface area is high, and the bar for fixing bugs, improving docs, and adding
tests is low.

## Before you open a pull request

- **Read [DESIGN.md](DESIGN.md).** Every architecture and API decision is
  recorded there with its rejected alternatives. A change that contradicts a
  decision needs to update the document with the new reasoning, not just the
  code. If the document and the code disagree, that's a bug — please report it.
- **Open an issue first for new features or API changes.** Non-goals are listed
  in the README and DESIGN.md §4; proposals that turn rankpipe into a
  recommender, feature store, model server, or workflow engine will be declined
  regardless of quality.
- Bug fixes, test coverage, benchmark additions, and documentation fixes do not
  need an issue.

## Development

```bash
go test -race -count=1 ./...     # tests, must pass under the race detector
go vet ./...
gofmt -l .                       # must print nothing
go test -run xxx -bench . ./...  # benchmarks
```

CI runs all of the above on the three most recent Go releases. There are no
external dependencies and pull requests that add one need a strong reason in
their description.

## What a good pull request looks like

- One logical change per PR.
- **Semantics changes come with tests that would fail without the change.**
  Ordering, determinism, cancellation, and failure-policy behaviour are
  contracts; a change to any of them needs a test and a DESIGN.md/README update.
- Performance changes come with `benchstat`-style before/after numbers from
  `go test -bench` in the PR description.
- Public identifiers have doc comments that state the contract (what the caller
  may rely on), not just what the code does.
- Commit messages explain *why*.

## Reporting bugs

Use the bug report issue template. A minimal reproducer as a Go test is the
most useful thing you can include: the pipeline definition, the input, the
expected output, and the actual output.

## Security

Please do not file security issues publicly; see [SECURITY.md](SECURITY.md).

## License

By contributing you agree that your contributions are licensed under the
[MIT License](LICENSE).
