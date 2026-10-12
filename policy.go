package throttle

import (
	"context"
	"errors"
	"fmt"
	"math"
	rand "math/rand/v2"
	"reflect"
	"strings"
	"time"
)

const (
	defaultBucketDuration = time.Second
	defaultBucketCount    = 120
	defaultMinimumSamples = 10
	defaultMaxResources   = 1_024

	// MaxBuckets bounds the memory and per-snapshot work used by one resource.
	MaxBuckets = 1_024
	// MaxResources bounds active resource histories per throttler, not histories
	// retained by caller-held permits after reset or eviction.
	MaxResources = 65_536
	// MaxResourceBytes bounds a resource identity retained in memory.
	MaxResourceBytes = 256
	// MaxRevisionBytes bounds the policy revision retained in snapshots and events.
	MaxRevisionBytes = 128
	// MaxWindowDuration bounds the configured rolling-window span. Expiry is
	// applied on state access; no background timer removes idle histories.
	MaxWindowDuration = 24 * time.Hour
	// MaxRetainedBuckets bounds configured aggregate active-history capacity.
	// Caller-held permits may retain retired generations outside this bound.
	MaxRetainedBuckets = 1 << 20
	// MaxPriorityLevels bounds trusted priority cardinality.
	MaxPriorityLevels = 8
)

// Clock supplies bounded, concurrency-safe timestamps for window expiration.
// Calls are synchronous, outside the state lock, and cannot be interrupted by
// admission cancellation. A panic falls back to wall-clock time; it does not
// guarantee admission. Returned times must fit time.Time.UnixNano's
// representable range.
type Clock interface {
	// Now returns a timestamp without unbounded work; callers may invoke it
	// concurrently.
	Now() time.Time
}

// Random supplies decision samples, not a cryptographic quota. Interpreting
// probability as an expected admission rate assumes independent uniform samples.
// Calls must be bounded and concurrency-safe. Invalid
// values (including negative zero), values outside [0, 1), and panics fail open
// for that decision; a blocking source is not preempted.
type Random interface {
	// Float64 returns an independent finite sample in [0, 1); implementations must
	// support concurrent calls.
	Float64() float64
}

// Completion borrows the admitted operation's context, result and error for one
// synchronous classifier call. A classifier must not retain these inputs; the
// throttler does not store operation payloads or errors.
type Completion struct {
	// Context is the borrowed context passed to the admitted operation.
	Context context.Context
	// Result is the borrowed operation result, including when Err is non-nil.
	Result any
	// Err is the original operation error; nil denotes success for the default
	// classifier.
	Err error
}

// Classifier maps admitted work to Accepted, DownstreamOverload,
// DownstreamFailure or Ignored. Explicit overload must represent known
// downstream evidence, not an ambiguous caller timeout or local rejection.
// Panics, unknown outcomes and LocalRejection become Ignored/ReasonUnspecified;
// reason bytes are not separately validated. Calls must be bounded,
// concurrency-safe and must not retain Completion.
type Classifier func(Completion) Classification

// Observer receives copied Event values synchronously after state locks are
// released. It may reenter the throttler; concurrent callers may invoke it
// concurrently and delivery order is not globally serialized. Panics are
// contained but blocking callbacks delay the caller. Nil disables observation.
// Callers own callback state and must keep it bounded and concurrency-safe.
type Observer func(Event)

// Priority is a bounded policy-defined level. Zero is the least privileged.
type Priority uint8

// PriorityResolver derives a configured level only from trusted caller context.
// It runs synchronously outside the state lock and must be
// bounded/concurrency-safe. A panic or out-of-range level falls back to zero;
// cancellation cannot preempt a blocking resolver.
type PriorityResolver func(context.Context) Priority

// PriorityPolicy scales shedding for optional trusted priority levels, not hard
// authorization or capacity controls. An empty scale with nil Resolve disables
// priority. Otherwise both are required, with at most MaxPriorityLevels
// entries; construction copies the slice but retains the resolver.
type PriorityPolicy struct {
	// RejectionScale contains finite values in [0, 1], beginning at 1 and
	// non-increasing. Level zero is least privileged; zero scales disable shedding
	// for that level.
	RejectionScale []float64
	// Resolve selects a trusted level into RejectionScale. Nil is valid only with
	// an empty scale.
	Resolve PriorityResolver
}

// WindowConfig defines bucket-granular rolling history. Zero fields select
// one-second buckets and 120 buckets. Negative duration/count or more than
// MaxBuckets buckets are invalid; the combined span must be at most
// MaxWindowDuration.
type WindowConfig struct {
	// BucketDuration is the positive width of each bucket; zero selects one
	// second.
	BucketDuration time.Duration
	// BucketCount is retained bucket cardinality; zero selects 120.
	BucketCount int
}

// Algorithm is a package-closed policy selector. NewPolicy accepts only a
// GoogleSRE value (not *GoogleSRE); nil selects GoogleSRE with multiplier two.
type Algorithm interface{ algorithm() }

// GoogleSRE uses max(0, (requests-K*accepts)/(requests+1)), as described in the
// Google SRE overload chapter. Requests include local rejection; ordinary
// downstream failures count as accepts, not overload. The sample threshold
// gates calculation; the strict probability cap bounds the result. This is
// probabilistic feedback, not an absolute quota.
type GoogleSRE struct {
	// AcceptMultiplier is K, finite in [1, 1000]. A zero GoogleSRE value is
	// invalid; a nil Algorithm selects K=2.
	AcceptMultiplier float64
}

func (GoogleSRE) algorithm() {}

// PolicyConfig contains construction inputs validated by NewPolicy. Revision and
// priority
// scales are copied; collaborator interfaces and function/closure state are
// retained, not deep-copied. Revision and both probability bounds are required.
// Nil collaborators select defaults where documented.
type PolicyConfig struct {
	// Revision is required, at most MaxRevisionBytes bytes, and visible in
	// snapshots/events. It must not contain secrets.
	Revision string
	// Window controls expiry resolution and per-resource allocation; bucket count
	// times resource capacity must not exceed MaxRetainedBuckets.
	Window WindowConfig
	// MinimumSamples gates shedding on completed downstream samples; zero selects
	// 10. Local rejections and ignored completions do not increase it.
	MinimumSamples uint64
	// Algorithm selects GoogleSRE; nil uses K=2.
	Algorithm Algorithm
	// MaxRejectionProbability is required, finite and strictly between zero and
	// one.
	MaxRejectionProbability float64
	// MinimumAdmissionProbability is required, finite and strictly between zero
	// and one. The effective cap is min(MaxRejectionProbability,
	// 1-MinimumAdmissionProbability); no fixed per-interval probe count is
	// promised.
	MinimumAdmissionProbability float64
	// MaxResources bounds active histories, not caller-held permits or downstream
	// concurrency. Zero selects 1024; at most 65536 resources and the configured
	// MaxRetainedBuckets combined capacity are admitted.
	MaxResources int
	// Clock supplies time; nil selects the system clock, typed nil is invalid.
	Clock Clock
	// Random supplies decision samples; nil selects math/rand, typed nil is
	// invalid.
	Random Random
	// Classifier maps Execute results; nil selects conservative default error
	// classification. Manual Record calls already supply their classification.
	Classifier Classifier
	// Observer receives post-lock events; nil disables callbacks.
	Observer Observer
	// DryRun admits would-reject decisions and increments DryRunRejections instead
	// of LocalRejections. Admitted work still requires completion recording.
	DryRun bool
	// Priority controls trusted rejection scaling; its zero value disables
	// priority.
	Priority PriorityPolicy
}

// Policy is immutable validated configuration created by NewPolicy. Its zero
// value is rejected by New. Copies construct independent histories but share
// caller collaborators, whose synchronization remains caller-owned.
type Policy struct{ config policyConfig }

type policyConfig struct {
	revision       string
	bucketDuration time.Duration
	bucketCount    int
	minimumSamples uint64
	acceptsK       float64
	maxProbability float64
	maxResources   int
	clock          Clock
	random         Random
	classifier     Classifier
	observer       Observer
	dryRun         bool
	priorityScale  []float64
	priority       PriorityResolver
}

// ConfigError identifies an invalid configuration or API argument.
// Package-created errors use fixed field/problem categories and retain no
// caller input. Manually constructed or modified fields are rendered verbatim
// by Error.
type ConfigError struct {
	// Field names the rejected option or API argument. Package-created values
	// contain no caller payload.
	Field string
	// Problem describes the fixed rejection category; it is not a nested cause.
	Problem string
}

// Error formats Field and Problem from a non-nil ConfigError; it does not wrap
// another error.
func (e *ConfigError) Error() string {
	return fmt.Sprintf("adaptive throttle: invalid %s: %s", e.Field, e.Problem)
}

func invalid(field, problem string) error { return &ConfigError{Field: field, Problem: problem} }

// NewPolicy validates defaults and bounds before creating an immutable policy,
// returning *ConfigError on rejection. It copies Revision and priority scales
// but retains collaborators. Nil Clock/Random choose wall-clock time/math/rand;
// typed-nil interfaces are invalid. A nil Classifier accepts nil errors and
// ignores all errors (matching known cancellation/deadline/local-policy reasons
// with errors.Is). Such matching can invoke caller Is/Unwrap code and is not a
// bounded traversal guarantee.
func NewPolicy(config PolicyConfig) (Policy, error) {
	if config.Revision == "" {
		return Policy{}, invalid("Revision", "must not be empty")
	}
	if len(config.Revision) > MaxRevisionBytes {
		return Policy{}, invalid("Revision", "exceeds maximum length")
	}

	window := config.Window
	if window.BucketDuration < 0 {
		return Policy{}, invalid("Window.BucketDuration", "must be positive")
	}
	if window.BucketDuration == 0 {
		window.BucketDuration = defaultBucketDuration
	}
	if window.BucketCount == 0 {
		window.BucketCount = defaultBucketCount
	}
	if window.BucketCount < 1 || window.BucketCount > MaxBuckets {
		return Policy{}, invalid("Window.BucketCount", "is outside the supported range")
	}
	if window.BucketDuration > MaxWindowDuration/time.Duration(window.BucketCount) {
		return Policy{}, invalid("Window", "exceeds maximum duration")
	}

	minimumSamples := config.MinimumSamples
	if minimumSamples == 0 {
		minimumSamples = defaultMinimumSamples
	}

	algorithm, ok := config.Algorithm.(GoogleSRE)
	if config.Algorithm == nil {
		algorithm = GoogleSRE{AcceptMultiplier: 2}
		ok = true
	}
	if !ok {
		return Policy{}, invalid("Algorithm", "is unsupported")
	}
	if !finite(algorithm.AcceptMultiplier) || algorithm.AcceptMultiplier < 1 || algorithm.AcceptMultiplier > 1_000 {
		return Policy{}, invalid("Algorithm.AcceptMultiplier", "must be finite and in [1, 1000]")
	}

	if !finite(config.MaxRejectionProbability) || config.MaxRejectionProbability <= 0 || config.MaxRejectionProbability >= 1 {
		return Policy{}, invalid("MaxRejectionProbability", "must be finite and in (0, 1)")
	}
	if !finite(config.MinimumAdmissionProbability) || config.MinimumAdmissionProbability <= 0 || config.MinimumAdmissionProbability >= 1 {
		return Policy{}, invalid("MinimumAdmissionProbability", "must be finite and in (0, 1)")
	}
	maximum := min(config.MaxRejectionProbability, 1-config.MinimumAdmissionProbability)

	maxResources := config.MaxResources
	if maxResources == 0 {
		maxResources = defaultMaxResources
	}
	if maxResources < 1 || maxResources > MaxResources {
		return Policy{}, invalid("MaxResources", "is outside the supported range")
	}
	if maxResources > MaxRetainedBuckets/window.BucketCount {
		return Policy{}, invalid("Window", "combined resource and bucket capacity is too large")
	}

	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	} else if nilInterface(clock) {
		return Policy{}, invalid("Clock", "must not be nil")
	}
	random := config.Random
	if random == nil {
		random = systemRandom{}
	} else if nilInterface(random) {
		return Policy{}, invalid("Random", "must not be nil")
	}
	classifier := config.Classifier
	if classifier == nil {
		classifier = defaultClassifier
	}
	priorityScale, priority, err := normalizePriority(config.Priority)
	if err != nil {
		return Policy{}, err
	}

	return Policy{config: policyConfig{
		revision:       strings.Clone(config.Revision),
		bucketDuration: window.BucketDuration,
		bucketCount:    window.BucketCount,
		minimumSamples: minimumSamples,
		acceptsK:       algorithm.AcceptMultiplier,
		maxProbability: maximum,
		maxResources:   maxResources,
		clock:          clock,
		random:         random,
		classifier:     classifier,
		observer:       config.Observer,
		dryRun:         config.DryRun,
		priorityScale:  priorityScale,
		priority:       priority,
	}}, nil
}

func normalizePriority(policy PriorityPolicy) ([]float64, PriorityResolver, error) {
	if len(policy.RejectionScale) == 0 {
		if policy.Resolve != nil {
			return nil, nil, invalid("Priority.RejectionScale", "is required with Resolve")
		}
		return nil, nil, nil
	}
	if len(policy.RejectionScale) > MaxPriorityLevels {
		return nil, nil, invalid("Priority.RejectionScale", "has too many levels")
	}
	if policy.Resolve == nil {
		return nil, nil, invalid("Priority.Resolve", "must not be nil")
	}
	// Copy policy data, not resolver state; reusing a Policy must not let later
	// caller slice edits alter priority, while collaborator ownership stays external.
	copyOfScale := append([]float64(nil), policy.RejectionScale...)
	for index, scale := range copyOfScale {
		if !finite(scale) || scale < 0 || scale > 1 {
			return nil, nil, invalid("Priority.RejectionScale", "values must be finite and in [0, 1]")
		}
		if index == 0 && scale != 1 {
			return nil, nil, invalid("Priority.RejectionScale", "lowest priority must have scale 1")
		}
		if index > 0 && scale > copyOfScale[index-1] {
			return nil, nil, invalid("Priority.RejectionScale", "must be non-increasing")
		}
	}
	return copyOfScale, policy.Resolve, nil
}

func defaultClassifier(completion Completion) Classification {
	switch {
	case completion.Err == nil:
		return Classification{Outcome: Accepted, Reason: ReasonSuccess}
	case errors.Is(completion.Err, context.Canceled):
		return Classification{Outcome: Ignored, Reason: ReasonCallerCanceled}
	case errors.Is(completion.Err, context.DeadlineExceeded):
		return Classification{Outcome: Ignored, Reason: ReasonCallerDeadline}
	case errors.Is(completion.Err, ErrRejected):
		return Classification{Outcome: Ignored, Reason: ReasonLocalPolicy}
	default:
		return Classification{Outcome: Ignored, Reason: ReasonUnspecified}
	}
}

func finite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func nilInterface(value any) bool {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type systemRandom struct{}

func (systemRandom) Float64() float64 {
	// #nosec G404 -- Samples govern probabilistic load shedding, not authentication, secret generation, or a hard security quota.
	return rand.Float64()
}
