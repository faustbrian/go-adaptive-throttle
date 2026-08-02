package throttle_test

import (
	"context"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

func TestBackwardClockJumpResetsHistoryEvenWithinBucket(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 900_000_000)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "clock-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 3},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0.99},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	clock.now = clock.now.Add(-500 * time.Millisecond)

	snapshot, ok := throttler.Snapshot("inventory")
	if !ok {
		t.Fatal("Snapshot() did not retain resource identity")
	}
	if snapshot.Requests != 0 || snapshot.Samples != 0 || snapshot.RejectionProbability != 0 || snapshot.WindowAge != 0 {
		t.Fatalf("Snapshot() after backward jump = %+v, want reset history", snapshot)
	}
}

func TestFirstAdmissionAfterIdleWindowDoesNotUseExpiredOverload(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "idle-admission-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 3},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	clock.now = clock.now.Add(3 * time.Second)

	permit, err := throttler.TryAcquire(context.Background(), "inventory")
	if err != nil {
		t.Fatalf("TryAcquire() error = %v, want admitted recovery probe", err)
	}
	if permit == nil {
		t.Fatal("TryAcquire() returned nil recovery permit")
	}
}

func TestRollingWindowExpiresBucketsAndReportsBoundedAge(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "roll-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 3},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0.99},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.DownstreamFailure}); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	clock.now = clock.now.Add(time.Second)
	partial, _ := throttler.Snapshot("inventory")
	if partial.Requests != 1 || partial.Accepts != 1 || partial.Failures != 1 || partial.WindowAge != time.Second {
		t.Fatalf("partial Snapshot() = %+v", partial)
	}
	clock.now = clock.now.Add(2 * time.Second)
	expired, _ := throttler.Snapshot("inventory")
	if expired.Requests != 0 || expired.WindowAge != 0 {
		t.Fatalf("expired Snapshot() = %+v, want idle reset", expired)
	}
}
