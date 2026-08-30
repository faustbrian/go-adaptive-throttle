# Changelog

All notable changes to this module are documented here.

## Unreleased

### Changed

- Replace copied repository tooling with the pinned `go-library-tools` v1.0.13
  contract while retaining package-owned policy and verification evidence.

### Documentation

- Replace the archived monorepo link with package-owned documentation.

## 1.0.0 - 2026-08-25

### Changed

- Exclude intentional nested modules from root local-proxy archives so local,
  bootstrap, CI, and public module checksums describe the same source
  boundary.

- Track the pinned documentation-tool lockfile so clean CI checkouts install
  the exact validated cspell dependency.

- Reconcile standalone dependency checksums against deterministic current
  module archives so CI, local verification, and release consumers resolve
  identical content.

- Harden standalone documentation validation with deterministic spelling and
  link checks, package-specific documentation gates, and repository-local
  contributor guidance.

### Documentation

- Link the package README to package-owned documentation.

### Added

- Process-local Google SRE requests-versus-accepts adaptive throttling with
  bounded rolling histories and guaranteed probabilistic probe flow.
- Explicit admission, execution, recording, classification, priority, dry-run,
  observer, reset, partition, and immutable snapshot contracts.
- Kubernetes, composition, tuning, security, simulation, benchmark, migration,
  API, and operations guidance.
- Deterministic rolling-window differential tests, fixed-stream statistical and
  lifecycle simulations, expanded fuzz/race/stress/fault/leak campaigns, and a
  pinned Failsafe-Go v0.9.6 probability and performance comparison.

### Changed

- Publish the module from its standalone `github.com/faustbrian/go-adaptive-throttle` identity while preserving its documented API and behavior.
- The default classifier now ignores every error; applications must explicitly
  select completed downstream failures or overload evidence. This prevents
  rejections from other local policies from contaminating downstream samples.
