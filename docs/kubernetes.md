# Kubernetes and horizontal scaling

Every `Throttler` is process-local. Pods can make different decisions because
they see different traffic, clocks, random streams, resource partitions, and
policy revisions. This module deliberately has no gossip or pseudo-distributed
mode.

## Lifecycle

- **Startup and scale-up:** a new pod has no history and admits until it reaches
  `MinimumSamples`. Use a conservative minimum, readiness ramp, upstream load
  balancing, or application-owned warm-up if cold admission could overload a
  dependency. Never copy opaque in-memory counters between pods.
- **Rolling update:** old and new revisions can coexist. Include the bounded
  policy revision in metrics and compare them separately.
- **Scale-down and abrupt death:** terminating a pod discards its samples.
  Stop routing new work during drain, allow owned requests to finish within the
  pod termination budget, then accept that unfinished permits produce no
  sample. Abrupt death loses all local evidence.
- **Maximum replicas:** capacity planning must assume the configured maximum
  replica count can independently emit its minimum probe flow and cold-start
  traffic.

## Fleet calculations

For pod `i` with incoming rate `R_i` and current rejection probability `p_i`,
expected fleet admission is:

```text
sum(R_i * (1 - p_i))
```

Using a single average probability is valid only when weighted by each pod's
offered load. Random outcomes and small sample windows create variance around
the expectation. Stable sharding should keep a resource on a bounded set of
pods when that resource maps to an independent downstream capacity pool, but
sharding does not make state distributed.

Aggregate counters by policy revision and bounded outcome/reason values. Do not
use resource identity, URL, error text, result, or tenant value as a label.

## HPA feedback

Rejection can lower local CPU while external demand rises. Rejection rate alone
is therefore unsafe as an HPA target and can form a positive feedback loop:
load rises, rejection rises, CPU falls, HPA scales down, and per-pod offered
load rises again. Use demand and downstream-capacity signals with a tested
control model. Validate scale-up reset, scale-down sample loss, stabilization
windows, readiness, and mixed revisions in simulation before production use.
