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

## Alerts and dashboards

Alert on downstream goodput and overload, not local rejection alone. Useful
diagnostics include prolonged cap saturation, zero admitted probes despite
offered traffic, unexpected ignored-result growth, partition churn, cold-start
overload, and divergent policy revisions. `Snapshots` is bounded but scans all
retained resources; do not call it on every request.

## Failure handling

- Invalid configuration is rejected before allocation.
- Backward or window-sized forward clock jumps reset history and fail open.
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
