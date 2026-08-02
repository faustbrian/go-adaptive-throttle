# Goal: Harden Adaptive Throttling

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT",
"SHOULD", "SHOULD NOT", "RECOMMENDED", "NOT RECOMMENDED", "MAY", and
"OPTIONAL" in this document are to be interpreted as described in BCP 14
[RFC2119] [RFC8174] when, and only when, they appear in all capitals, as
shown here.

## Objective

Prove mathematical correctness, statistical behavior, bounded state, recovery,
classification safety, and multi-replica operational behavior.

## Required Campaigns

- Compare every probability calculation and bucket transition with a
  deterministic reference model.
- Use fixed random streams to prove exact decisions at probability boundaries.
- Simulate healthy, partial failure, overload ramp, sudden outage, recovery,
  sparse traffic, burst traffic, oscillation, and correlated replica traffic.
- Verify minimum samples, maximum rejection, probe flow, idle reset, dry-run,
  and policy reconfiguration.
- Fuzz durations, counters, K/threshold values, probabilities, clock movement,
  classifiers, partitions, and event sequences.
- Race admission, record, cancellation, bucket rollover, snapshot, reset,
  eviction, and shutdown.
- Prove local rejection and other policy rejections never become downstream
  samples unless explicitly selected.
- Simulate cold pod scale-up, mixed revisions, scale-down, SIGTERM, abrupt
  death, and misleading HPA signals.

Statistical tests MUST be deterministic or use justified confidence bounds with
fixed seeds and non-flaky sample sizes.

## Comparative Evidence

Benchmark and simulate equivalent policies against Failsafe-Go and reference
Google SRE equations. Publish admission quality, downstream goodput, rejection,
recovery time, probability error, CPU, memory, allocations, lock contention,
and observer overhead. Do not compare different failure classifiers or sample
windows as if they were equivalent.

## Completion Gates

- Meaningful exact 100% statement coverage and exactly 100% viable mutation
  kills.
- Race, fuzz, simulation, stress, leak, fault, benchmark, API compatibility,
  docs, security, and supply-chain gates pass.
- Cross-policy tests prove no rejection feedback loop or retry amplification.
- Final review finds no probability overflow, permanent blackout, hidden
  global state, unbounded partitioning, false cluster guarantee, or flaky
  statistical evidence.

[RFC2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC8174]: https://www.rfc-editor.org/rfc/rfc8174
