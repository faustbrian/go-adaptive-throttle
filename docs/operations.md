# Operations, tuning, simulation, and security

## Tuning

Start with the Google SRE reference `K=2`, a window long enough to contain
representative downstream decisions, and a minimum sample count that prevents
sparse-traffic noise. Short buckets improve expiry resolution but increase
per-resource memory and snapshot work. Large K delays shedding; small K reacts
more aggressively. The maximum probability is an overload-safety bound, while
minimum admission preserves recovery observations.

Use dry run first. Graph offered requests, downstream samples, accepts,
explicit overloads, ordinary failures, ignored results, local and dry-run
rejections, probability, admitted work, latency, and downstream goodput.
Rejection is not success: the objective is increased useful downstream
throughput and bounded latency under overload.

## Scenario simulation

Before rollout, replay or synthesize at least:

- healthy traffic and isolated ordinary failures;
- partial overload, a gradual ramp, and sudden outage;
- recovery through only the configured probe flow;
- sparse, bursty, oscillating, and correlated replica traffic;
- invalid random values and forward/backward clock movement;
- classifier mistakes and local-policy errors;
- partition churn at the cardinality limit;
- cold pod scale-up, drain, abrupt death, scale-down, and mixed revisions; and
- HPA signals where rejection reduces CPU while offered demand increases.

Use fixed random streams for exact boundary decisions. Statistical experiments
must use fixed seeds and documented confidence bounds. Compare policies only
when their classifiers, windows, offered load, and failure semantics are
equivalent.

The executable campaign includes a bucket-by-bucket differential reference
model, a 5,292-state Failsafe-Go probability grid, exact random samples
immediately below, at, and above the decision boundary, a 100,000-draw fixed
seed statistical experiment, concurrent reset/eviction/rollover/cancellation,
goleak process-exit checks, repeated fault and stress targets, and deterministic
fleet/HPA models. Run `make fuzz`, `make race`, `make stress`, `make fault`, and
`make leak` for the focused campaigns.

The package starts no goroutines and owns no background resources, so shutdown
has no asynchronous state transition to race. SIGTERM drain is modeled by
stopping admission and optionally calling `ResetAll`; dropping a throttler
models abrupt process death. Outstanding permits are generation-bound and
cannot recreate reset or discarded histories.

## Alerts and dashboards

Alert on downstream goodput and overload, not local rejection alone. Useful
diagnostics include prolonged cap saturation, zero admitted probes despite
offered traffic, unexpected ignored-result growth, partition churn, cold-start
overload, and divergent policy revisions. `Snapshots` is bounded but scans all
retained resources; do not call it on every request.

## Failure handling

- Invalid configuration is rejected before allocation.
- Backward or window-sized forward clock jumps reset history and fail open.
- Injected-clock panics use current wall-clock time; normal window expiry and
  admission rules still apply rather than guaranteeing admission.
- Random-source anomalies admit the current request.
- Classifier anomalies become ignored results.
- Priority anomalies fall back to least privileged traffic.
- Observer panics are contained and cannot reverse a decision.
- Saturated counters and non-finite probability calculations do not produce
  total rejection.

## Security

Treat classifier and priority policy as trusted code. Never derive elevated
priority directly from an unauthenticated header, tenant string, or request
field. Bound and normalize resource identities before calling the package; the
library additionally enforces byte length and count bounds.

Observers receive no results, errors, URLs, tenants, or resource strings. Do
not reconstruct high-cardinality labels outside the package. Policy revisions
must not contain secrets. The library performs no network IO, starts no
goroutines, stores no credentials, and has no external runtime dependencies.

### Threat model and accepted residual risks

Model version 1, reviewed 2026-09-30 for the stable v1 module.

The protected assets are downstream availability and probe flow, the
integrity of process-local overload history, bounded process CPU and memory,
and the confidentiality of application results and resource identities.
Resource identities may originate at an untrusted request boundary; policy
configuration and injected collaborators are trusted application code.

| Risk | Severity | Owner | Rationale | Mitigation | Review condition |
| --- | --- | --- | --- | --- | --- |
| A clock, random source, priority resolver, classifier, or observer can block its synchronous caller. | Medium | Application integrator | The package cannot preempt arbitrary caller code without transferring goroutine and shutdown ownership. | Keep collaborators bounded and concurrency-safe, and enforce any external IO deadline before entering them. | Revisit if collaborators gain owned asynchronous execution or accept a new cancellation contract. |
| A cyclic or non-terminating custom error traversal can stall the default classifier. | Medium | Application integrator | Go error matching intentionally invokes caller-defined `Is` and `Unwrap` behavior, which v1 preserves. | Supply a classifier that uses bounded trusted signals when operation errors can be adversarial. | Revisit in a planned v2 classification contract that can define bounded traversal without changing v1 matching semantics. |
| Adversarial churn across valid resource identities makes eviction scan up to `MaxResources`. | Medium | Application integrator | Exact keys avoid collision-based history merging, while deterministic bounded eviction currently uses a linear scan. | Normalize identities to a finite capacity boundary and keep `MaxResources` at the smallest operational value. | Revisit if identities become directly attacker-controlled, eviction latency breaches its budget, or the maximum increases. |
| Caller-held permits can retain the throttler and retired resource histories beyond active-map limits. | Low | Application integrator | Permits belong to the caller and remain generation-bound after reset or eviction; the package does not track or terminate their lifetime. | Complete and discard permits promptly, bound downstream concurrency and queueing, and do not retain completed permits. | Revisit if the package begins owning outstanding work or promises a limit covering caller-held references. |
