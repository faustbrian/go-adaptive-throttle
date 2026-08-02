package throttle

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"
)

func TestPolicyExactValidBoundaries(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*PolicyConfig){
		"revision": func(config *PolicyConfig) {
			config.Revision = strings.Repeat("r", MaxRevisionBytes)
		},
		"one bucket": func(config *PolicyConfig) {
			config.Window = WindowConfig{BucketDuration: MaxWindowDuration, BucketCount: 1}
		},
		"maximum buckets": func(config *PolicyConfig) {
			config.Window = WindowConfig{BucketDuration: MaxWindowDuration / MaxBuckets, BucketCount: MaxBuckets}
			config.MaxResources = MaxRetainedBuckets / MaxBuckets
		},
		"minimum multiplier": func(config *PolicyConfig) {
			config.Algorithm = GoogleSRE{AcceptMultiplier: 1}
		},
		"maximum multiplier": func(config *PolicyConfig) {
			config.Algorithm = GoogleSRE{AcceptMultiplier: 1_000}
		},
		"maximum rejection below one": func(config *PolicyConfig) {
			config.MaxRejectionProbability = math.Nextafter(1, 0)
		},
		"minimum admission below one": func(config *PolicyConfig) {
			config.MinimumAdmissionProbability = math.Nextafter(1, 0)
		},
		"minimum resources": func(config *PolicyConfig) {
			config.MaxResources = 1
		},
		"maximum resources": func(config *PolicyConfig) {
			config.Window = WindowConfig{BucketDuration: time.Second, BucketCount: 1}
			config.MaxResources = MaxResources
		},
		"maximum priority levels": func(config *PolicyConfig) {
			config.Priority = PriorityPolicy{
				RejectionScale: []float64{1, 1, 0.75, 0.5, 0.25, 0.1, 0.01, 0},
				Resolve:        func(context.Context) Priority { return 0 },
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config := validPolicyConfig()
			mutate(&config)
			if _, err := NewPolicy(config); err != nil {
				t.Fatalf("NewPolicy() error = %v", err)
			}
		})
	}
}

func TestNewChecksEveryZeroPolicyInvariant(t *testing.T) {
	t.Parallel()

	policy, err := NewPolicy(validPolicyConfig())
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	if _, err := New(policy); err != nil {
		t.Fatalf("New(valid) error = %v", err)
	}
	oneBucketConfig := validPolicyConfig()
	oneBucketConfig.Window.BucketCount = 1
	oneBucketPolicy, err := NewPolicy(oneBucketConfig)
	if err != nil {
		t.Fatalf("NewPolicy(one bucket) error = %v", err)
	}
	if _, err := New(oneBucketPolicy); err != nil {
		t.Fatalf("New(one bucket) error = %v", err)
	}
	tests := map[string]func(*Policy){
		"revision":        func(policy *Policy) { policy.config.revision = "" },
		"clock":           func(policy *Policy) { policy.config.clock = nil },
		"random":          func(policy *Policy) { policy.config.random = nil },
		"bucket duration": func(policy *Policy) { policy.config.bucketDuration = 0 },
		"bucket count":    func(policy *Policy) { policy.config.bucketCount = 0 },
		"resources":       func(policy *Policy) { policy.config.maxResources = 0 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			invalidPolicy := policy
			mutate(&invalidPolicy)
			if throttler, err := New(invalidPolicy); err == nil || throttler != nil {
				t.Fatalf("New(invalid) = (%v, %v)", throttler, err)
			}
		})
	}
}

func TestResourceOrderingBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		left  resourceState
		right resourceState
		want  bool
	}{
		{left: resourceState{lastUsed: 1, slot: 2}, right: resourceState{lastUsed: 2, slot: 1}, want: true},
		{left: resourceState{lastUsed: 2, slot: 1}, right: resourceState{lastUsed: 2, slot: 2}, want: true},
		{left: resourceState{lastUsed: 2, slot: 2}, right: resourceState{lastUsed: 2, slot: 2}, want: false},
		{left: resourceState{lastUsed: 3, slot: 1}, right: resourceState{lastUsed: 2, slot: 2}, want: false},
	}
	for _, test := range tests {
		if got := resourcePrecedes(&test.left, &test.right); got != test.want {
			t.Fatalf("resourcePrecedes(%+v, %+v) = %t, want %t", test.left, test.right, got, test.want)
		}
	}
}

func TestEveryBucketFieldIsObservableData(t *testing.T) {
	t.Parallel()

	tests := []bucket{
		{requests: 1},
		{accepts: 1},
		{samples: 1},
		{overloads: 1},
		{failures: 1},
		{ignored: 1},
		{localRejections: 1},
		{dryRunRejections: 1},
	}
	if bucketHasData(&bucket{}) {
		t.Fatal("empty bucket reported data")
	}
	for _, candidate := range tests {
		candidate := candidate
		if !bucketHasData(&candidate) {
			t.Fatalf("bucketHasData(%+v) = false", candidate)
		}
	}
}

func TestNumericalDecisionBoundaries(t *testing.T) {
	t.Parallel()

	config := validPolicyConfig()
	config.MaxRejectionProbability = 0.9
	config.MinimumAdmissionProbability = 0.25
	configured, err := NewPolicy(config)
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	if configured.config.maxProbability != 0.75 {
		t.Fatalf("effective maximum probability = %v, want 0.75", configured.config.maxProbability)
	}

	policy := policyConfig{minimumSamples: 1, acceptsK: 1, maxProbability: 0.5}
	if got := rejectionProbability(Snapshot{Requests: 1, Samples: 1}, policy); got != math.Nextafter(0.5, 0) {
		t.Fatalf("probability at cap = %.17g", got)
	}
	total := uint64(math.MaxUint64 - 1)
	saturatingAdd(&total, 1)
	if total != math.MaxUint64 {
		t.Fatalf("non-overflowing boundary sum = %d", total)
	}
	total = math.MaxUint64
	saturatingAdd(&total, 1)
	if total != math.MaxUint64 {
		t.Fatalf("overflowing sum = %d", total)
	}
	if err := validateResource(strings.Repeat("r", MaxResourceBytes)); err != nil {
		t.Fatalf("validateResource(max) error = %v", err)
	}
}

type boundaryRandom struct{ value float64 }

func (random boundaryRandom) Float64() float64 { return random.value }

func TestRandomPriorityAndTimeExactBoundaries(t *testing.T) {
	t.Parallel()

	for _, value := range []float64{0, math.Nextafter(1, 0)} {
		if got := safeRandom(boundaryRandom{value: value}); got != value {
			t.Fatalf("safeRandom(%v) = %v", value, got)
		}
	}
	for _, value := range []float64{-math.SmallestNonzeroFloat64, 1, math.Inf(1)} {
		if got := safeRandom(boundaryRandom{value: value}); got != 1 {
			t.Fatalf("safeRandom(%v) = %v, want fail-open 1", value, got)
		}
	}
	if got := safePriority(nil, context.Background(), 2); got != 0 {
		t.Fatalf("safePriority(nil, levels) = %d", got)
	}
	if got := safePriority(func(context.Context) Priority { return 1 }, context.Background(), 0); got != 0 {
		t.Fatalf("safePriority(resolver, zero levels) = %d", got)
	}
	if got := safePriority(func(context.Context) Priority { return 1 }, context.Background(), 2); got != 1 {
		t.Fatalf("safePriority(last valid) = %d", got)
	}
	if got := safePriority(func(context.Context) Priority { return 2 }, context.Background(), 2); got != 0 {
		t.Fatalf("safePriority(first invalid) = %d", got)
	}
	if tick := windowTick(time.Unix(-1, 0), time.Second); tick != -1 {
		t.Fatalf("windowTick(exact negative) = %d", tick)
	}
	if tick := windowTick(time.Unix(0, 0), time.Second); tick != 0 {
		t.Fatalf("windowTick(zero) = %d", tick)
	}
	if forwardGapAtLeast(5, 5, 2) || forwardGapAtLeast(5, 4, 2) || forwardGapAtLeast(5, 6, 2) {
		t.Fatal("forwardGapAtLeast() crossed a false boundary")
	}
	if !forwardGapAtLeast(5, 7, 2) {
		t.Fatal("forwardGapAtLeast() missed exact boundary")
	}
}
