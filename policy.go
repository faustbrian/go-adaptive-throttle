package throttle

import (
	"context"
	"errors"
	"fmt"
	"math"
	rand "math/rand/v2"
	"reflect"
	"time"
)

const (
	defaultBucketDuration = time.Second
	defaultBucketCount    = 120
	defaultMinimumSamples = 10
	defaultMaxResources   = 1_024

	// MaxBuckets bounds the memory and per-snapshot work used by one resource.
	MaxBuckets = 1_024
	// MaxResources bounds independently retained overload histories.
	MaxResources = 65_536
	// MaxResourceBytes bounds a resource identity retained in memory.
	MaxResourceBytes = 256
	// MaxRevisionBytes bounds the policy revision retained in snapshots and events.
	MaxRevisionBytes = 128
	// MaxWindowDuration bounds how long overload evidence can remain relevant.
	MaxWindowDuration = 24 * time.Hour
	// MaxRetainedBuckets bounds worst-case aggregate bucket allocation.
	MaxRetainedBuckets = 1 << 20
	// MaxPriorityLevels bounds trusted priority cardinality.
	MaxPriorityLevels = 8
)

// Clock supplies timestamps for rolling-window expiration.
type Clock interface {
	Now() time.Time
}

// Random supplies independent samples. Values must be finite and in [0, 1).
type Random interface {
	Float64() float64
}

// Completion is the ephemeral input to a classifier. Classifiers must not
// retain Context, Result, or Err after returning.
type Completion struct {
	Context context.Context
	Result  any
	Err     error
}

// Classifier maps one admitted completion to one bounded classification.
type Classifier func(Completion) Classification

// Observer receives immutable events after the state lock is released.
type Observer func(Event)

// Priority is a bounded policy-defined level. Zero is the least privileged.
type Priority uint8

// PriorityResolver derives priority from trusted caller context.
type PriorityResolver func(context.Context) Priority

// PriorityPolicy scales rejection probabilities for trusted priority levels.
// RejectionScale must begin at 1 and be non-increasing.
type PriorityPolicy struct {
	RejectionScale []float64
	Resolve        PriorityResolver
}

// WindowConfig defines a fixed-size rolling time window.
type WindowConfig struct {
	BucketDuration time.Duration
	BucketCount    int
}

// Algorithm is a closed set of probability policies supported by this module.
type Algorithm interface{ algorithm() }

// GoogleSRE uses max(0, (requests-K*accepts)/(requests+1)).
type GoogleSRE struct {
	AcceptMultiplier float64
}

func (GoogleSRE) algorithm() {}

// PolicyConfig contains construction inputs that are copied into an immutable Policy.
type PolicyConfig struct {
	Revision                    string
	Window                      WindowConfig
	MinimumSamples              uint64
	Algorithm                   Algorithm
	MaxRejectionProbability     float64
	MinimumAdmissionProbability float64
	MaxResources                int
	Clock                       Clock
	Random                      Random
	Classifier                  Classifier
	Observer                    Observer
	DryRun                      bool
	Priority                    PriorityPolicy
}

// Policy is an immutable, validated throttling policy.
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

// ConfigError reports one invalid policy field without retaining caller data.
type ConfigError struct {
	Field   string
	Problem string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("adaptive throttle: invalid %s: %s", e.Field, e.Problem)
}

func invalid(field, problem string) error { return &ConfigError{Field: field, Problem: problem} }

// NewPolicy validates and copies config into an immutable policy.
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
		revision:       config.Revision,
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
		return Classification{Outcome: DownstreamFailure, Reason: ReasonDownstreamFailure}
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

func (systemRandom) Float64() float64 { return rand.Float64() }
