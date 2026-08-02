package throttle_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

type fixedClock struct{ now time.Time }

func (c *fixedClock) Now() time.Time { return c.now }

type fixedRandom struct{ value float64 }

func (r fixedRandom) Float64() float64 { return r.value }

func TestGoogleSREProbabilityAndDeterministicRejection(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision: "v1",
		Window: throttle.WindowConfig{
			BucketDuration: time.Second,
			BucketCount:    60,
		},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1.5},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                8,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0.1},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}

	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for range 4 {
		if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.Accepted}); err != nil {
			t.Fatalf("Record(accepted) error = %v", err)
		}
	}
	for range 4 {
		if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
			t.Fatalf("Record(overload) error = %v", err)
		}
	}

	snapshot, ok := throttler.Snapshot("inventory")
	if !ok {
		t.Fatal("Snapshot() did not find resource")
	}
	wantProbability := 2.0 / 9.0 // max(0, (requests - K*accepts) / (requests + 1)).
	if math.Abs(snapshot.RejectionProbability-wantProbability) > 1e-15 {
		t.Fatalf("RejectionProbability = %.17g, want %.17g", snapshot.RejectionProbability, wantProbability)
	}

	permit, err := throttler.TryAcquire(context.Background(), "inventory")
	if !errors.Is(err, throttle.ErrRejected) {
		t.Fatalf("TryAcquire() error = %v, want ErrRejected", err)
	}
	if permit != nil {
		t.Fatal("TryAcquire() returned a permit for rejected work")
	}
}
