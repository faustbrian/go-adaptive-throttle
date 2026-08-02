# Benchmarks and comparative evidence

## Reproducibility

Published results below were collected on 2026-08-02 with Go 1.26.5,
darwin/arm64, Apple M4 Max, and `GOMAXPROCS=16`. Failsafe-Go is pinned at
v0.9.6. The comparison uses one resource, a two-minute window, 10 minimum
samples, `K=2`, a 50% Failsafe-Go failure threshold, a 0.9 maximum, and the
same success-only healthy classifier. Each operation acquires and records one
success.

```sh
go test -run='^$' \
  -bench='^(BenchmarkTryAcquireAndRecordHealthy|BenchmarkTryAcquireAndRecordWithObserver|BenchmarkFailsafeGoEquivalentHealthy|BenchmarkEquivalentHealthyContention|BenchmarkGoogleSREEquation)$' \
  -benchmem -benchtime=500ms -count=5 .
```

Median results from the five samples are:

| Workload | Implementation | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| healthy acquire and record | adaptive-throttle | 1,919 | 48 | 1 |
| healthy acquire and record | Failsafe-Go | 50.09 | 0 | 0 |
| 16-way contended acquire and record | adaptive-throttle | 2,455 | 48 | 1 |
| 16-way contended acquire and record | Failsafe-Go | 161.3 | 0 | 0 |
| standalone Google SRE equation | adaptive-throttle | 5.767 | 0 | 0 |

The adaptive-throttle operation additionally performs resource lookup,
bounded bucket aggregation, immutable permit ownership, snapshot accounting,
and application-request feedback. Each Failsafe-Go throttler uses one history
and does not provide equivalent resource partitions or snapshot data;
the latency comparison is therefore an operational cost comparison, not a
claim that the APIs do identical work.

The synchronous no-op observer retained 48 B/op and one allocation per
operation. Its five-sample range was 1,620-1,872 ns/op versus
1,717-3,300 ns/op without the observer, so this run did not resolve a stable
latency delta. Observer allocation overhead was zero; a latency claim requires
additional controlled samples rather than interpreting the noisy ordering.

Two-second CPU, allocation, and mutex profiles of the contended workloads
confirmed the expected lock boundary. Adaptive-throttle measured 3,962 ns/op,
48 B/op, and one allocation; bucket aggregation and saturating sums dominated
owned CPU, and `TryAcquire` owned 46.5 MB of the 47 MB package allocation
sample. Failsafe-Go measured 196.9 ns/op and zero benchmark allocations; its
profile was dominated by mutex and wall-clock work. Mutex profiles identified
the single policy lock as the contention owner in both implementations.

## Probability and policy comparison

`TestEquivalentFailsafeGoPolicyMatchesGoogleSREProbabilityGrid` directly runs
Failsafe-Go v0.9.6 and adaptive-throttle across 5,292 aligned states:

- `K` values 1, 1.25, 2, and 4;
- minimum samples 1, 5, and 20;
- every combination of 0-20 accepts and 0-20 overloads; and
- equal one-minute, 20-bucket windows and a 0.99 maximum.

The maximum absolute probability error was exactly zero. A separate direct
traffic-phase simulation compared 240 consecutive healthy, partial-failure,
sudden-outage, and recovery states and also had zero error, with a peak
probability of 0.283688. This proves the algebraic mapping for aligned completed
histories. It does not compare post-rejection feedback: the libraries
intentionally account local rejection differently.

## Admission quality and recovery

The fixed-stream end-to-end simulation uses seed `0xd1b54a32d192ed03`, 200
healthy offers, 400 sudden-outage offers, then recovery until probability
returns to zero. It produced:

| offered | admitted | downstream goodput | downstream overload | local rejection | recovery offers | peak probability |
|---:|---:|---:|---:|---:|---:|---:|
| 880 | 804 | 440 | 364 | 76 | 280 | 0.331667 |

The dry-run statistical campaign used 100,000 draws from a separately fixed
xorshift stream. For probability 0.555556 it observed 0.554090; the two-sided
Hoeffding bound was 0.010348 at `alpha=1e-9`. The test is deterministic and the
bound is stated rather than tuned to the observed count.

## Profiling and publication policy

For a new publication, retain raw benchmark output and CPU, heap, allocation,
and mutex profiles. Record Go version, OS, architecture, CPU, `GOMAXPROCS`,
benchmark duration/count, policy, bucket layout, resource count, classifier,
random source, observer, and corpus. Use `benchstat` when enough independent
samples are available to support a latency comparison.

Do not present Failsafe-Go traffic behavior as identical after a local
rejection, and do not compare policies with different classifiers, windows,
minimum samples, caps, priorities, or offered-load histories as equivalent.
