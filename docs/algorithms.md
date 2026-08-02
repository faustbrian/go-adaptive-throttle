# Algorithms and numerical behavior

## Google SRE requests versus accepts

This module implements the client-side adaptive-throttling model in the
[Google SRE overload chapter](https://sre.google/sre-book/handling-overload/).
For the current rolling window:

- `requests` is application work represented to the throttler, including work
  rejected locally by this throttler;
- `accepts` is admitted downstream work classified as `Accepted` or
  `DownstreamFailure`;
- `samples` is admitted work classified as `Accepted`,
  `DownstreamOverload`, or `DownstreamFailure`; and
- `K` is `GoogleSRE.AcceptMultiplier`.

Ignored results do not change requests, accepts, samples, or overloads. An
ordinary downstream failure counts as accepted by the downstream boundary but
not successful. An explicit downstream overload counts as a request and a
sample, but not an accept. Local rejection counts as an application request,
as required by the SRE equation, but never as a downstream sample or failure.

Before `MinimumSamples`, the probability is zero. Afterwards the raw
probability is:

```text
max(0, (requests - K * accepts) / (requests + 1))
```

The extra one in the denominator is the SRE stabilization term. With eight
requests, three accepts, and `K=2`, probability is `(8-6)/9 = 2/9`. Reducing K
makes shedding start earlier; increasing K makes it less aggressive.

`TryAcquire` rejects exactly when an injected random value in `[0,1)` is less
than the priority-adjusted probability. Equality admits. Invalid, non-finite,
out-of-range, or panicking random sources fail open for that decision.

## Bounds and probe flow

`MaxRejectionProbability` must be finite and in `(0,1)`.
`MinimumAdmissionProbability` must be finite and in `(0,1)`. The effective cap
is the smaller of the configured maximum and `1-minimumAdmission`; the returned
probability is clamped to the greatest representable value strictly below that
cap. This preserves a positive expected probe flow. It is a probability
guarantee, not a per-interval request guarantee: short random sequences can
still admit more or fewer probes.

All counters saturate at `math.MaxUint64`. Sums saturate, division uses finite
`float64` operands, and any non-finite intermediate fails open to probability
zero. Windows span at most 24 hours, and the configured bucket count, resource
count, combined retained buckets, identity length, revision length, priority
count, and window allocation are bounded before allocation.

## Rolling window and idle reset

The window contains `BucketCount` fixed buckets of `BucketDuration`. A bucket
expires when its tick falls outside the current set of ticks. A forward jump of
at least the full window resets the history. Any backward clock movement,
including movement inside one bucket, also resets history. This fail-open
choice avoids retaining stale overload evidence or risking a blackout.

`WindowAge` is bucket-granular age of the oldest retained non-empty bucket and
is zero when the window contains no observations. It never exceeds the window
span. Times are interpreted through `time.Time.UnixNano`; callers must keep
injected times in that method's documented representable range.

## Decisions, reset, and policy changes

The probability for an admission decision is calculated from completed
history before that decision. A rejected request is then added to `requests`
and `LocalRejections`. An admitted request becomes a request and sample only
when its permit is recorded; abandoned or evicted permits produce no sample.
Every permit records at most once.

`Reset(resource)` deletes exactly one history. `ResetAll` deletes all bounded
histories. Outstanding permits for an evicted or reset history become inert;
they never recreate that identity and never merge into a newer resource.

Policies and throttlers are not reconfigured in place. Constructing a new
policy and throttler starts fresh local history. Applications that need a
transition must run explicitly named old and new revisions side by side or
accept the documented cold-start reset.

## Algorithm choice

Only the Google SRE requests-versus-accepts algorithm is implemented.
Failsafe-Go v0.9.6 exposes its configuration as a failure-rate threshold, but
its implementation computes the same equation for an aligned completed history
when `K = 1 / successRateThreshold`, equivalently
`failureRateThreshold = 1 - 1/K`. The direct differential campaign covers
5,292 aligned states with zero probability error.

The policies stop being dynamically equivalent after local rejection:
`adaptive-throttle` counts the application attempt in `requests`, as the SRE
model specifies, while Failsafe-Go v0.9.6 does not add rejected acquisitions to
its execution statistics. Comparative results therefore identify the aligned
history boundary and do not claim identical feedback behavior.
