# Changelog

All notable changes to this module are documented here.

## Unreleased

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

- The default classifier now ignores every error; applications must explicitly
  select completed downstream failures or overload evidence. This prevents
  rejections from other local policies from contaminating downstream samples.
