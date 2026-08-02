package throttle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

type priorityContextKey struct{}

func TestTrustedBoundedPriorityScalesRejection(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "priority-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 10},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                3,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0.2},
		Priority: throttle.PriorityPolicy{
			RejectionScale: []float64{1, 0.25},
			Resolve: func(ctx context.Context) throttle.Priority {
				priority, _ := ctx.Value(priorityContextKey{}).(throttle.Priority)
				return priority
			},
		},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for _, resource := range []string{"low", "critical", "invalid"} {
		if err := throttler.Record(resource, throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
			t.Fatalf("Record(%q) error = %v", resource, err)
		}
	}

	if _, err := throttler.TryAcquire(context.Background(), "low"); !errors.Is(err, throttle.ErrRejected) {
		t.Fatalf("low priority error = %v, want ErrRejected", err)
	}
	critical := context.WithValue(context.Background(), priorityContextKey{}, throttle.Priority(1))
	if permit, err := throttler.TryAcquire(critical, "critical"); err != nil || permit == nil {
		t.Fatalf("critical priority = (%v, %v), want admitted", permit, err)
	}
	invalid := context.WithValue(context.Background(), priorityContextKey{}, throttle.Priority(99))
	if _, err := throttler.TryAcquire(invalid, "invalid"); !errors.Is(err, throttle.ErrRejected) {
		t.Fatalf("invalid priority error = %v, want fail-safe low-priority rejection", err)
	}
}
