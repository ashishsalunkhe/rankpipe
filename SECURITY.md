# Security Policy

## Scope

rankpipe is a pure-Go library with no network I/O, no file I/O, no
dependencies outside the standard library, and no unsafe code. It executes
caller-supplied callbacks over caller-supplied data. The security-relevant
guarantees it makes are:

- No goroutine outlives a `Run` call (no leaked work).
- Bounded memory and goroutine usage as documented (`WithConcurrency`,
  `Limiter`); no hidden unbounded queues.
- `Run` never modifies the caller's input slice.
- Cancellation and deadlines are honoured at every point rankpipe controls.

A violation of any of these — for example a panic or unbounded allocation
reachable from adversarial candidate counts or scores (NaN, ±Inf, huge K), or
a goroutine leak — is treated as a security issue.

## Supported versions

Only the latest tagged release receives fixes.

## Reporting a vulnerability

Please **do not** open a public issue. Report privately via
[GitHub's private vulnerability reporting](https://github.com/ashishsalunkhe/rankpipe/security/advisories/new)
or by email to avsalunkhe98@gmail.com.

Include a minimal reproducer if you can. You will get an acknowledgement
within 7 days and a fix or a documented decision within 30 days for confirmed
issues. Credit is given in the release notes unless you prefer otherwise.
