# adaptive-throttle

`adaptive-throttle` is a process-local probabilistic load shedder for Go. It
uses recent downstream admission signals to reject a bounded share of new work
before network execution while always preserving probabilistic probe flow.

It is not a circuit breaker, fixed rate limiter, concurrency limiter, retry
budget, bulkhead, authorization quota, autoscaler, or distributed coordinator.

## Quick start

```go
policy, err := throttle.NewPolicy(throttle.PolicyConfig{
    Revision: "catalog-v1",
    Window: throttle.WindowConfig{
        BucketDuration: time.Second,
        BucketCount: 120,
    },
    MinimumSamples: 20,
    Algorithm: throttle.GoogleSRE{AcceptMultiplier: 2},
    MaxRejectionProbability: 0.9,
    MinimumAdmissionProbability: 0.1,
    MaxResources: 1_000,
})
if err != nil {
    return err
}

throttler, err := throttle.New(policy)
if err != nil {
    return err
}

value, err := throttle.Execute(ctx, throttler, "catalog-api", fetchCatalog)
```

The default classifier treats success as accepted and ignores every error.
Applications must explicitly classify proven downstream failures and
vendor-specific overload results. This conservative default prevents local
rate-limit, bulkhead, breaker, retry, and other policy rejections from becoming
downstream samples.

## Design guarantees

- The Google SRE requests-versus-accepts equation is specified exactly.
- Rejection probability is finite, non-negative, and strictly below the
  effective configured maximum.
- Locally rejected work never runs and never becomes a downstream sample.
- Rolling histories, priorities, resource identities, snapshots, and events
  have explicit cardinality bounds.
- State and random decisions are local to one process. No fleet-wide guarantee
  is implied.
- Injected clocks, randomness, classifiers, priority resolvers, and observers
  make decisions reproducible without global state.

## Documentation

- [Algorithms and numerical behavior](docs/algorithms.md)
- [API, classification, priority, and migration](docs/api.md)
- [Composition](docs/composition.md)
- [Kubernetes and fleet behavior](docs/kubernetes.md)
- [Operations, tuning, simulation, and security](docs/operations.md)
- [Benchmarks and comparison policy](docs/benchmarks.md)
- [FAQ](docs/faq.md)

## Verification

```sh
make check
```

The repository gate runs exact coverage, race, fuzz, mutation, API,
documentation, vulnerability, license, SBOM, and clean-consumer checks.

## References

- [Google SRE, Handling Overload](https://sre.google/sre-book/handling-overload/)
- [Failsafe-Go adaptive throttler](https://failsafe-go.dev/adaptive-throttler/)
- [Go context package](https://pkg.go.dev/context)
- [Go memory model](https://go.dev/ref/mem)

## License

MIT. See [LICENSE](LICENSE).

## Ecosystem

Use the [Golib documentation portal](https://github.com/faustbrian/golib/blob/main/docs/index.md)
to choose companion packages, supported stacks, recipes, and operations guidance.
