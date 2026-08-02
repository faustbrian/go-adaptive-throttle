package throttle

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

type unsupportedAlgorithm struct{}

func (unsupportedAlgorithm) algorithm() {}

type nilClock struct{}

func (*nilClock) Now() time.Time { return time.Time{} }

type nilRandom struct{}

func (*nilRandom) Float64() float64 { return 0 }

func validPolicyConfig() PolicyConfig {
	return PolicyConfig{
		Revision:                    "v1",
		Window:                      WindowConfig{BucketDuration: time.Second, BucketCount: 10},
		MinimumSamples:              1,
		Algorithm:                   GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       systemClock{},
		Random:                      systemRandom{},
	}
}

func TestNewPolicyRejectsUnsafeConfiguration(t *testing.T) {
	t.Parallel()

	tests := map[string]func(*PolicyConfig){
		"empty revision":           func(c *PolicyConfig) { c.Revision = "" },
		"long revision":            func(c *PolicyConfig) { c.Revision = strings.Repeat("r", MaxRevisionBytes+1) },
		"negative bucket duration": func(c *PolicyConfig) { c.Window.BucketDuration = -1 },
		"negative bucket count":    func(c *PolicyConfig) { c.Window.BucketCount = -1 },
		"too many buckets":         func(c *PolicyConfig) { c.Window.BucketCount = MaxBuckets + 1 },
		"window duration overflow": func(c *PolicyConfig) {
			c.Window = WindowConfig{BucketDuration: time.Duration(math.MaxInt64), BucketCount: 2}
		},
		"window is not recent": func(c *PolicyConfig) {
			c.Window = WindowConfig{BucketDuration: MaxWindowDuration, BucketCount: 2}
		},
		"unsupported algorithm":   func(c *PolicyConfig) { c.Algorithm = unsupportedAlgorithm{} },
		"small multiplier":        func(c *PolicyConfig) { c.Algorithm = GoogleSRE{AcceptMultiplier: 0.9} },
		"large multiplier":        func(c *PolicyConfig) { c.Algorithm = GoogleSRE{AcceptMultiplier: 1_001} },
		"NaN multiplier":          func(c *PolicyConfig) { c.Algorithm = GoogleSRE{AcceptMultiplier: math.NaN()} },
		"zero maximum":            func(c *PolicyConfig) { c.MaxRejectionProbability = 0 },
		"unit maximum":            func(c *PolicyConfig) { c.MaxRejectionProbability = 1 },
		"NaN maximum":             func(c *PolicyConfig) { c.MaxRejectionProbability = math.NaN() },
		"zero minimum admission":  func(c *PolicyConfig) { c.MinimumAdmissionProbability = 0 },
		"large minimum admission": func(c *PolicyConfig) { c.MinimumAdmissionProbability = 1.1 },
		"NaN minimum admission":   func(c *PolicyConfig) { c.MinimumAdmissionProbability = math.NaN() },
		"no rejection range":      func(c *PolicyConfig) { c.MinimumAdmissionProbability = 1 },
		"negative resources":      func(c *PolicyConfig) { c.MaxResources = -1 },
		"too many resources":      func(c *PolicyConfig) { c.MaxResources = MaxResources + 1 },
		"too many retained buckets": func(c *PolicyConfig) {
			c.Window.BucketCount = MaxBuckets
			c.MaxResources = MaxRetainedBuckets/MaxBuckets + 1
		},
		"typed nil clock":         func(c *PolicyConfig) { c.Clock = (*nilClock)(nil) },
		"typed nil random":        func(c *PolicyConfig) { c.Random = (*nilRandom)(nil) },
		"resolver without levels": func(c *PolicyConfig) { c.Priority.Resolve = func(context.Context) Priority { return 0 } },
		"too many priority levels": func(c *PolicyConfig) {
			c.Priority = PriorityPolicy{RejectionScale: make([]float64, MaxPriorityLevels+1), Resolve: func(context.Context) Priority { return 0 }}
		},
		"levels without resolver": func(c *PolicyConfig) { c.Priority.RejectionScale = []float64{1} },
		"NaN priority scale": func(c *PolicyConfig) {
			c.Priority = PriorityPolicy{RejectionScale: []float64{1, math.NaN()}, Resolve: func(context.Context) Priority { return 0 }}
		},
		"privileged lowest priority": func(c *PolicyConfig) {
			c.Priority = PriorityPolicy{RejectionScale: []float64{0.5}, Resolve: func(context.Context) Priority { return 0 }}
		},
		"increasing priority scale": func(c *PolicyConfig) {
			c.Priority = PriorityPolicy{RejectionScale: []float64{1, 0.5, 0.75}, Resolve: func(context.Context) Priority { return 0 }}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			config := validPolicyConfig()
			mutate(&config)
			_, err := NewPolicy(config)
			var configErr *ConfigError
			if !errors.As(err, &configErr) || configErr.Error() == "" {
				t.Fatalf("NewPolicy() error = %v, want ConfigError", err)
			}
		})
	}
}

func TestNewPolicyAppliesBoundedDefaults(t *testing.T) {
	t.Parallel()

	policy, err := NewPolicy(PolicyConfig{
		Revision:                    "defaults-v1",
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	permit, err := throttler.TryAcquire(context.Background(), "default")
	if err != nil || permit == nil {
		t.Fatalf("TryAcquire() = (%v, %v), want admission", permit, err)
	}
}
