# adaptive-throttle

[![CI](https://github.com/faustbrian/go-adaptive-throttle/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/faustbrian/go-adaptive-throttle/actions/workflows/ci.yml)
[![CodeQL](https://img.shields.io/badge/CodeQL-required-blue)](https://github.com/faustbrian/go-adaptive-throttle/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Mutation](https://img.shields.io/badge/mutation-100%25_required-blue)](CONTRIBUTING.md#verification)
[![Documentation](https://img.shields.io/badge/docs-checked_in_CI-blue)](docs/)
[![Go Reference](https://pkg.go.dev/badge/github.com/faustbrian/go-adaptive-throttle.svg)](https://pkg.go.dev/github.com/faustbrian/go-adaptive-throttle)
[![Release](https://img.shields.io/github/v/release/faustbrian/go-adaptive-throttle?sort=semver)](https://github.com/faustbrian/go-adaptive-throttle/releases)
[![Go](https://img.shields.io/badge/go-1.26.6-00ADD8?logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`adaptive-throttle` is a process-local probabilistic load shedder for Go. It
uses recent downstream admission signals to reject a bounded share of new work
before network execution while always preserving probabilistic probe flow.

It is not a circuit breaker, fixed rate limiter, concurrency limiter, retry
budget, bulkhead, authorization quota, autoscaler, or distributed coordinator.

The module is a stable v1 public library. It requires Go 1.26.6 or newer.

## Install

```sh
go get github.com/faustbrian/go-adaptive-throttle@v1
```

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

## Lifecycle and ownership

A `Throttler` owns only bounded in-memory state. It starts no goroutines,
performs no network I/O, and owns no external resources, so it requires no
shutdown or close operation. A `Throttler` is safe for concurrent use. Callers
own operation contexts and downstream work; injected clocks, randomness,
classifiers, priority resolvers, and observers must support concurrent calls.

## Package map

| Import path | Package | Role | Release status |
| --- | --- | --- | --- |
| `github.com/faustbrian/go-adaptive-throttle` | `throttle` | Process-local adaptive admission | Stable v1 root module |

## Documentation

- [Documentation index](docs/README.md)
- [Algorithms and numerical behavior](docs/algorithms.md)
- [API, classification, priority, and migration](docs/api.md)
- [Composition](docs/composition.md)
- [Kubernetes and fleet behavior](docs/kubernetes.md)
- [Operations, tuning, simulation, and security](docs/operations.md)
- [Rejection and overload troubleshooting](docs/operations.md#failure-handling)
- [Benchmarks and comparison policy](docs/benchmarks.md)
- [FAQ](docs/faq.md)
- [Support](SUPPORT.md)
- [Security policy and reporting guidance](SECURITY.md)
- [Compatibility policy](COMPATIBILITY.md)
- [Release history](CHANGELOG.md)

Shared construction, ownership, lifecycle, and composition expectations follow
the versioned [Golib ecosystem index](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/README.md)
and its [Resilience family](https://github.com/faustbrian/go-library-tools/blob/v1.4.0/docs/ecosystem/design-language.md#package-families-and-selection).

## Verification

```sh
make ci
```

The complete repository gate validates configuration, inventory, cohesion,
repository and workflow contracts, online specification authorities, exact
coverage, race, fuzz, mutation, API, documentation, vulnerability, license,
SBOM, and clean-consumer checks. Use `make check` for the implementation gates
alone.

## References

- [Google SRE, Handling Overload](https://sre.google/sre-book/handling-overload/)
- [Failsafe-Go adaptive throttler](https://failsafe-go.dev/adaptive-throttler/)
- [Go context package](https://pkg.go.dev/context)
- [Go memory model](https://go.dev/ref/mem)

## License

MIT. See [LICENSE](LICENSE).
