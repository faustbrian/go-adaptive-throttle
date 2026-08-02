# Composition

Adaptive throttling belongs immediately above the downstream operation whose
overload signals it observes. The following order is a starting point, not a
universal transport stack:

```text
authentication and authorization
  -> fixed quota or inbound rate limit
  -> cache
  -> retry or hedge policy that will not retry ErrRejected
  -> adaptive throttle per downstream attempt
  -> hard bulkhead or adaptive concurrency limit
  -> breaker
  -> per-attempt timeout
  -> downstream network operation
```

- **Cache:** serve cache hits before adaptive admission. Cache misses are the
  work that can load the downstream.
- **Rate limit and quota:** enforce caller entitlement independently and do not
  classify their rejections as downstream overload.
- **Retry and hedge:** when these wrap adaptive admission, `ErrRejected` must be
  terminal by default; a new immediate attempt has the same local evidence and
  amplifies demand. Each actual downstream attempt may be recorded separately.
- **Bulkhead and adaptive concurrency:** keep hard capacity controls inside
  adaptive admission so recovery probes cannot bypass them. Their local
  rejections are ignored or ordinary local failures, not downstream overload.
- **Circuit breaker:** a breaker is a distinct all-or-nothing state machine.
  Its open rejection must not be mapped to downstream overload. Decide whether
  the breaker protects each attempt or the complete logical operation and keep
  that scope explicit.
- **Timeout:** per-attempt timeouts belong adjacent to the network operation.
  End-to-end caller deadlines remain outside. Neither is overload evidence
  without explicit service-specific proof.

If a retry policy is inside one adaptive permit, only the final result becomes
one sample and intermediate overload signals are hidden. If adaptive admission
is inside the retry policy, every network attempt is sampled, but retry must
exclude `ErrRejected`. Prefer the latter when per-attempt signals are reliable.

Hedges consume independent downstream capacity and should acquire independent
adaptive permits. Cancelled losing hedges are ignored. A hedge must not be
started merely because another hedge received `ErrRejected`.
