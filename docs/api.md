# API, classification, priority, and migration

## Construction

`NewPolicy` validates and copies configuration. Callers cannot mutate its
priority scales after construction. `New` accepts only a policy created by
`NewPolicy`; the zero `Policy` is rejected. A `Throttler` is safe for concurrent
use. Injected collaborators must themselves support concurrent calls if the
same instance is shared by callers or by multiple throttlers.

`Revision` is a bounded operator-selected policy identifier. `MaxResources`
bounds exact resource identities; identities are retained internally to avoid
hash-collision history merging. Snapshots and events expose only a bounded
process-local numeric slot, never the arbitrary identity. A slot is stable
while its history is retained and is deterministically reusable after reset or
eviction, keeping metric-label cardinality bounded.

## Admission and recording

`TryAcquire(ctx, resource)` returns a `Permit` or `ErrRejected`. Context
cancellation is checked before resource state is created. Rejected work must
not run. Record an admitted result once with `Permit.Record`.

`Record(resource, classification)` is for integrations that already control
admission and completion. It represents one complete result. Recording
`LocalRejection` changes only application request and local-rejection counts;
it does not create a downstream sample.

Generic `Execute` performs acquire, invokes the operation only after admission,
classifies the result, and records once. It does not retry, hedge, sleep,
recover panics, or change the operation error.

## Classification

| Outcome | Requests | Accepts | Samples | Meaning |
|---|---:|---:|---:|---|
| `Accepted` | +1 | +1 | +1 | downstream accepted and operation succeeded |
| `DownstreamOverload` | +1 | 0 | +1 | explicit downstream overload evidence |
| `DownstreamFailure` | +1 | +1 | +1 | downstream accepted work but failed ordinarily |
| `Ignored` | 0 | 0 | 0 | excluded cancellation, deadline, or local policy result |
| `LocalRejection` | +1 | 0 | 0 | application attempt shed before downstream execution |

The default classifier never infers overload from a generic error. It ignores
`context.Canceled`, `context.DeadlineExceeded`, and `ErrRejected`; it treats any
other error as an ordinary downstream failure. Rate limiter, bulkhead, breaker,
and other local-policy errors therefore do not become overload evidence unless
the application explicitly maps them to `DownstreamOverload`.

Timeouts are ambiguous. Map one to overload only when the timeout scope proves
it measured downstream saturation rather than caller budget, queue wait, or
network failure. Authentication, authorization, validation, not-found, and
business rejections must remain accepted ordinary results or ignored results,
not overload.

Classifier inputs are ephemeral. Do not retain their context, result, or error.
Classifier panics and invalid classifications fail safe to `Ignored`.

## Priority

Priority is optional. `PriorityPolicy.Resolve` is the only priority source, so
untrusted request fields cannot elevate themselves through `TryAcquire`.
Configure at most eight scales. Level zero is the least privileged and must
use scale 1; later levels must be non-increasing values in `[0,1]`. Invalid or
panicking resolutions fall back to level zero.

All levels retain the global minimum admission probability. Priority can
reduce shedding for critical work but cannot bypass a rate limit, bulkhead, or
other hard safety control. Resource partitioning, rather than priority, should
isolate histories that represent incompatible downstream capacity pools.

## Observers and dry run

An observer receives bounded `Event` values synchronously after locks are
released. It may inspect snapshots or call the throttler reentrantly. It cannot
change the decision already made. Observer panics are contained. Keep observer
work bounded because it adds caller latency.

Events contain numeric outcome, reason, decision, priority, policy revision,
probability, resource slot, and an immutable snapshot. They never contain the
resource key, result, error string, URL, or tenant value.

Dry run evaluates the same random decision but converts a would-reject into an
admission. `DryRunRejections` and `DecisionDryRunAdmit` expose the counterfactual
without adding a local rejection. The admitted operation must still be
recorded.

## Migration

1. Identify the exact downstream capacity boundary and a bounded stable
   resource identity.
2. Start in dry run with a conservative window, minimum samples, and cap.
3. Explicitly classify only known overload responses.
4. Compare admitted goodput, downstream overloads, latency, and dry-run
   decisions; do not tune on rejection count alone.
5. Enable shedding at a low cap, then raise it while checking recovery probes.
6. Version policy changes through `Revision`; creating a new throttler resets
   local history.

Do not migrate a circuit breaker, authorization quota, or fixed rate limit into
this package. Compose those independent controls instead.
