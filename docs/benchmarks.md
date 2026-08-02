# Benchmarks and comparison policy

The benchmark suite measures healthy acquire-and-record with and without a
synchronous observer and the standalone Google SRE equation. It reports
nanoseconds and allocations through Go's `testing` benchmark harness.

```sh
go test -run='^$' -bench=. -benchmem -count=5
```

For publication, record Go version, operating system, architecture, CPU,
`GOMAXPROCS`, benchmark duration/count, policy, bucket layout, resource count,
classifier, random source, observer, and corpus. Use `benchstat` across at least
five independent samples. Profile CPU, heap, allocations, and mutex contention
at healthy, overloaded, rollover, and maximum-cardinality workloads.

Failsafe-Go's documented adaptive throttler uses a failure-rate-threshold
algorithm rather than this module's Google requests-versus-accepts equation.
The two are not presented as equivalent benchmark competitors. A future
comparison is valid only if this module adds a distinctly named threshold
algorithm and both sides use the same window, classification, priority, and
offered-load model. Until then, compare the SRE equation against the deterministic
reference tests and compare operational outcomes only as explicitly different
policies.
