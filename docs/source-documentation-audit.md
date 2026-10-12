# Source documentation audit

## Scope and contract

This Tier A audit began before source edits at remote-main base
`2ee36bcccd5a856f6d8d39fa6d77bf6bb728ab39`. The sole module and package are
`github.com/faustbrian/go-adaptive-throttle` and `throttle`. Toolchain: Go 1.27.2.
The original checkout's pending implementation is excluded and preserved.
Only comments and directly related maintained prose may change; executable
AST/tokens, directives, API signatures and historical evidence must remain fixed.

Reproduce the inventory with `git ls-files '*.go'` and
`golib source-docs inventory --json`; reconcile `modules.json`/`packages.json`.
The two production files own the public contracts and private coordination.
All 23 test files are included, not only public declaration owners.

## Initial gaps and selected risks

The initial checker reports 66 gaps: public option/result fields, enum values,
interface methods and error methods, plus two private collaborator methods
that should not be inflated merely to satisfy current checker overreach.
The existing package comment does not explain lifecycle, probabilistic rather
than hard quotas, admission-versus-completion counting or callback ownership.

The source/test pass must establish:

- Required probability/revision settings versus zero-selected defaults; copied
  strings/scales versus retained caller collaborators and closure state.
- SRE requests/accepts/sample semantics, finite probability cap and equality
  admission, sparse-history fail-open behavior and saturating counters.
- Context checkpoints without preempting mutex wait or synchronous collaborators;
  no owned goroutines, timers or shutdown handles. Caller cancellation does not
  complete a permit, recover an operation panic, or force downstream cessation.
- Mutex ownership, callbacks outside locks, permit one-shot completion and
  generation identity preventing reset/eviction resurrection. Caller-held permits
  can retain retired state beyond active-map limits.
- Resource identity byte/cardinality/combined-bucket bounds, deterministic LRU-like
  eviction, slot reuse, observer concurrency/order and bounded classifier reasons.
- Default error classification, anomalous collaborator recovery, trusted priority,
  dry-run counterfactuals and private ring-window/negative-time/clock-jump rationale.

Authorities: [API](api.md), [algorithms](algorithms.md),
[operations and residual risks](operations.md), tests and maintained repository guidance.
Historical performance/comparison evidence and source pins remain immutable.
No behavior, conformance or performance qualification is inferred from prose.

## Tracked Go coverage

| File | Role | Variant |
| --- | --- | --- |
| `benchmark_test.go` | test | ordinary |
| `boundary_test.go` | test | ordinary |
| `classifier_internal_test.go` | test | ordinary |
| `comparison_test.go` | test | ordinary |
| `concurrency_test.go` | test | ordinary |
| `example_test.go` | test | ordinary |
| `execute_test.go` | test | ordinary |
| `fuzz_test.go` | test | ordinary |
| `internal_invariant_test.go` | test | ordinary |
| `leak_test.go` | test | ordinary |
| `model_differential_test.go` | test | ordinary |
| `model_test.go` | test | ordinary |
| `mutation_boundaries_internal_test.go` | test | ordinary |
| `observer_test.go` | test | ordinary |
| `operational_test.go` | test | ordinary |
| `partition_test.go` | test | ordinary |
| `policy.go` | source | ordinary |
| `policy_internal_test.go` | test | ordinary |
| `policy_test.go` | test | ordinary |
| `priority_test.go` | test | ordinary |
| `resource_retention_test.go` | test | !race allocation/retention variant |
| `simulation_test.go` | test | ordinary |
| `throttle.go` | source | ordinary |
| `throttle_test.go` | test | ordinary |
| `window_test.go` | test | ordinary |

## Surface, variants and tooling

Both production files' exported declarations, constants, fields and interface
methods are audited; the final native AST inventory records qualified members.
All private coordination paths and test helpers are inspected for rationale.
Six fuzz targets, five benchmarks and two Examples require useful boundary,
oracle and measurement notes. No saved fuzz fixtures or generation directives/generated Go exist. The sole
build constraint excludes resource-retention measurements from race builds; it
is preserved. No debt/deprecation markers were found. Historical inputs stay fixed.

The configured API baseline uses apidiff (`api/baseline.txt`); there is no native
API snapshot generator. The native docs target is
`make -f verification/package.mk docs`, including package rendering, local links,
cleanup-error handling and the two executable Examples. Selected verification
also includes formatting, parsed AST/executable-token/directive equivalence,
rendered member inspection, source-doc checks, changed-content secret inspection
and ordinary complete-diff review. No unchanged runtime/race/leak/fuzz/benchmark
campaign is needed. Go compilation requires the coordinator's serialized slot
and three disposable owned caches.


## Declaration and member inventory

The native AST inventory counts 104 public declarations/members plus two
exported methods on private default collaborators; those private methods are
reviewed for interface conformance, not inflated with redundant prose.

- `policy.go`: `MaxBuckets`, `MaxResources`, `MaxResourceBytes`, `MaxRevisionBytes`, `MaxWindowDuration`, `MaxRetainedBuckets`, `MaxPriorityLevels`, `Clock`, `Clock.Now`, `Random`, `Random.Float64`, `Completion`, `Completion.Context`, `Completion.Result`, `Completion.Err`, `Classifier`, `Observer`, `Priority`, `PriorityResolver`, `PriorityPolicy`, `PriorityPolicy.RejectionScale`, `PriorityPolicy.Resolve`, `WindowConfig`, `WindowConfig.BucketDuration`, `WindowConfig.BucketCount`, `Algorithm`, `GoogleSRE`, `GoogleSRE.AcceptMultiplier`, `PolicyConfig`, `PolicyConfig.Revision`, `PolicyConfig.Window`, `PolicyConfig.MinimumSamples`, `PolicyConfig.Algorithm`, `PolicyConfig.MaxRejectionProbability`, `PolicyConfig.MinimumAdmissionProbability`, `PolicyConfig.MaxResources`, `PolicyConfig.Clock`, `PolicyConfig.Random`, `PolicyConfig.Classifier`, `PolicyConfig.Observer`, `PolicyConfig.DryRun`, `PolicyConfig.Priority`, `Policy`, `ConfigError`, `ConfigError.Field`, `ConfigError.Problem`, `ConfigError.Error`, `NewPolicy`, `systemClock.Now`, `systemRandom.Float64`.
- `throttle.go`: `ErrRejected`, `Outcome`, `Accepted`, `DownstreamOverload`, `DownstreamFailure`, `Ignored`, `LocalRejection`, `Reason`, `ReasonUnspecified`, `ReasonSuccess`, `ReasonExplicitOverload`, `ReasonDownstreamFailure`, `ReasonCallerCanceled`, `ReasonCallerDeadline`, `ReasonLocalPolicy`, `Classification`, `Classification.Outcome`, `Classification.Reason`, `Decision`, `DecisionRecorded`, `DecisionAdmit`, `DecisionReject`, `DecisionDryRunAdmit`, `DecisionReset`, `Event`, `Event.Decision`, `Event.Outcome`, `Event.Reason`, `Event.Probability`, `Event.Priority`, `Event.ResourceSlot`, `Event.Snapshot`, `Snapshot`, `Snapshot.Revision`, `Snapshot.Requests`, `Snapshot.Accepts`, `Snapshot.Samples`, `Snapshot.Overloads`, `Snapshot.Failures`, `Snapshot.Ignored`, `Snapshot.LocalRejections`, `Snapshot.DryRunRejections`, `Snapshot.RejectionProbability`, `Snapshot.WindowAge`, `Snapshot.ResourceSlot`, `Throttler`, `New`, `Throttler.Record`, `Throttler.TryAcquire`, `Permit`, `Permit.Record`, `Execute`, `Throttler.Snapshot`, `Throttler.Snapshots`, `Throttler.Reset`, `Throttler.ResetAll`.

## Findings and final verification

Every tracked source/test file was read. Public contracts now distinguish
copied revision/scales from retained collaborators; application requests from
completed downstream samples; unscaled snapshot versus priority-adjusted event
probability; numeric slot reuse from generation identity; and active-map limits
from caller-retained retired permits. Reason bytes are not restricted to the
named values. Nil/zero object, operation panic, callback recovery and synchronous
cancellation boundaries are explicit. Existing conservative default error
traversal can block on hostile caller Is/Unwrap implementations, as already
recorded in the maintained threat model; no new traversal guarantee is invented.

Internal comments explain context methods outside the mutex, separate one-shot
permit ownership, generation pointer identity, saturated recency tie-breaking,
strict representable probability caps, negative-time floor division and window
reset rationale. Fuzz/benchmark and complex fixture notes identify actual
boundaries rather than describing assertion syntax. The parallel benchmark's
one-time setup is not explicitly excluded, and peer/clock/feedback differences
are retained instead of claiming unrestricted equivalence.

Selected Go work used Go 1.27.2, GOWORK=off, bounded compile parallelism and
three task-owned disposable caches:

- Pinned `gofmt -l` over the 25 tracked Go files: passed.
- Native Go parsed AST comparison excluding comments/positions, scanner token
  equality and exact directive comparison against the recorded base: all 25
  files preserve executable structure, including the !race variant. A later
  constant-comment correction reuses this unchanged-executable proof.
- `go doc -all .`: rendered and inspected across the complete root API, including
  callbacks, fields, defaults, error methods, permits and classification. A
  short final refresh covers corrected active-history/window constants.
- `make -f verification/package.mk docs`: passed local links/anchors, package
  rendering, error handling and both executable Examples. No other runtime test
  was selected. The apidiff baseline is unchanged; no prose snapshot exists.
- `golib source-docs check --json`: zero findings after the semantic owner pass.
- Complete final-diff inspection, `git diff --check`, build/generator/marker
  inventory and changed-content secret inspection: passed.

The Go caches were removed immediately after each bounded run. Helpers and
rendered outputs are deleted after capturing these results. No documentation
blocker remains. Broad runtime, statistical/model, race/leak, fuzz, benchmark,
mutation, consumer and release campaigns were not run for unchanged code. The
original checkout remains untouched; the committed detached worktree is retained
for coordinator remote-main/CI delivery. No worker publication or module release
is performed.

## Objective enforcement adoption

The local `source-docs-check` target is required by ordinary/full checks and CI.
Ordinary shared CI uses `source_documentation: check` with immutable Tools
v2.4.0 and matching authenticated binary/checksum declaration. Separately
selected qualification source identities and gates remain unchanged. Structural
checks do not replace this maintained semantic audit or native docs/examples.
