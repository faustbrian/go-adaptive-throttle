# Goal: Adaptive Throttling And Load Shedding

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in BCP 14
[RFC2119] [RFC8174] when, and only when, they appear in all capitals, as
shown here.

## Objective

Build `adaptive-throttle` as a production-grade process-local probabilistic
load shedder. It MUST react to recent downstream overload signals by rejecting
a bounded proportion of new work before network execution while preserving
probe traffic so recovery remains observable.

## Authoritative References

- Google SRE client-side throttling and overload handling:
  https://sre.google/sre-book/handling-overload/
- Failsafe-Go adaptive throttler:
  https://failsafe-go.dev/adaptive-throttler/
- primary probability, rolling-window, and overload-control references used by
  implemented algorithms;
- Go context, time, random, memory-model, race, fuzz, and profiling docs.

## Product Boundary

Adaptive throttling sheds some calls based on recent overload. It is not a
circuit breaker, fixed rate limit, adaptive concurrency limiter, retry budget,
bulkhead, authorization quota, or Kubernetes autoscaler.

The module MUST NOT count its own local rejection as a downstream failure.
Errors from rate limits, bulkheads, breakers, caller cancellation, and local
deadlines MUST be excluded by default unless the caller explicitly classifies
them as downstream overload evidence.

## Core Model

Provide immutable policies containing:

- bounded recent sample window and bucket layout;
- minimum sample count before shedding;
- failure/overload classifier;
- threshold or algorithm-specific aggressiveness parameter;
- maximum rejection probability strictly below one;
- minimum probe/admission flow;
- deterministic clock and random source;
- stable bounded resource identity;
- optional bounded priority policy; and
- observer and dry-run settings.

Provide `TryAcquire`, context-aware execution, explicit `Record` APIs, and
immutable snapshots. Each admitted execution MUST produce at most one sample.
Rejected work MUST not invoke the protected operation.

## Algorithms

The package MUST first implement and specify a Google SRE-style requests versus
accepts algorithm or another primary-source equivalent. If it also implements
a failure-rate threshold algorithm like Failsafe-Go, the two MUST have distinct
names, equations, semantics, and tests.

For every algorithm document:

- exact inputs and classification;
- rolling-window expiration and idle reset;
- rejection-probability equation;
- minimum samples and recovery probes;
- bounds and numerical precision;
- clock-jump behavior;
- random sampling method; and
- reset, snapshot, and reconfiguration behavior.

Probability MUST remain finite and within `[0, max)` for every input. Integer
overflow, NaN/Inf, stale buckets, and random-source anomalies MUST fail safely
without turning into total rejection.

## Classification

Callers MUST classify results as accepted/successful, downstream overload,
ordinary downstream failure, ignored, or local rejection. HTTP/RPC adapters MAY
provide conservative defaults for explicit overload responses such as `429`,
`Retry-After`, and selected availability errors, but vendor policy remains
caller controlled.

Timeouts require explicit classification because they may represent downstream
overload, client deadline misconfiguration, queue wait, or network failure.
Authentication, authorization, validation, not-found, and business rejection
MUST NOT be overload by default.

## Priority And Fairness

Optional priority shedding MAY preserve critical work, but priority must come
from a trusted caller policy. It MUST be bounded, observable, and protected
against arbitrary elevation. Low-priority traffic MUST retain a documented
minimum flow or explicitly accept starvation as a named policy.

Tenant/resource partitioning MUST have bounded cardinality and deterministic
eviction that does not silently merge incompatible overload histories.

## Kubernetes And Horizontal Scaling

Throttler state and random decisions are pod-local. Replicas can make different
decisions from different traffic samples. Scale-up resets local history and may
temporarily admit more traffic; scale-down removes samples; rolling updates may
mix policy revisions.

Docs MUST cover conservative startup, aggregate admission calculations, stable
resource sharding, maximum replicas, drain behavior, metric aggregation, and
HPA feedback loops. Rejection may lower CPU even while external demand rises,
so rejection rate alone MUST NOT be recommended as an HPA target without a
tested control model.

The package MUST NOT offer a pseudo-distributed throttler through pod gossip.

## Composition

Document and test placement with cache, rate limit, bulkhead, adaptive
concurrency, breaker, retry, hedge, and timeout scopes. Retry and hedge MUST not
treat adaptive rejection as evidence that a new immediate attempt is useful by
default. Probe traffic MUST still pass through hard capacity safety controls.

## Observability

Expose bounded snapshots and events for request/accept/failure samples,
current probability, admitted, rejected, dry-run rejection, window age,
priority, classifier reason, and policy revision. Provide injectable randomness
for reproducible decisions.

No arbitrary resource key, result, error string, URL, or tenant value may
become a metric label. Observers execute outside locks and cannot alter the
decision already made.

## Documentation And Automation

Document equations, worked examples, algorithm choice, classification,
priority, startup/recovery, Kubernetes behavior, HPA caveats, composition,
tuning, simulation, API, migration, FAQ, security, operations, benchmarks, and
changelog.

Require meaningful exact 100% statement coverage, exactly 100% viable mutation
kills, equation/reference tests, deterministic probability tests, simulation,
race, fuzz, leak, fault, benchmark, API compatibility, docs, security,
supply-chain, and clean-consumer gates.

## Acceptance Criteria

- Rejection probability is correct, finite, bounded, and reproducible in tests.
- Local rejection never contaminates downstream overload history.
- Probe flow allows recovery detection.
- State and partition cardinality remain bounded.
- Kubernetes lifecycle and fleet-level limitations are explicit.

[RFC2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC8174]: https://www.rfc-editor.org/rfc/rfc8174
