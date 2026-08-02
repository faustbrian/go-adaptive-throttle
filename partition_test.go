package throttle_test

import (
	"context"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

func TestPartitionsEvictDeterministicallyWithoutMergingOutstandingPermits(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "partition-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 10},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                2,
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
	oldPermit, err := throttler.TryAcquire(context.Background(), "oldest")
	if err != nil {
		t.Fatalf("TryAcquire(oldest) error = %v", err)
	}
	if err := throttler.Record("retained", throttle.Classification{Outcome: throttle.Accepted}); err != nil {
		t.Fatalf("Record(retained) error = %v", err)
	}
	if err := throttler.Record("retained", throttle.Classification{Outcome: throttle.Accepted}); err != nil {
		t.Fatalf("Record(retained touch) error = %v", err)
	}
	if err := throttler.Record("new", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
		t.Fatalf("Record(new) error = %v", err)
	}
	if _, ok := throttler.Snapshot("oldest"); ok {
		t.Fatal("oldest resource survived deterministic LRU eviction")
	}
	before, _ := throttler.Snapshot("new")
	if err := oldPermit.Record(throttle.Classification{Outcome: throttle.Accepted}); err != nil {
		t.Fatalf("recording evicted permit error = %v", err)
	}
	after, _ := throttler.Snapshot("new")
	if after != before {
		t.Fatalf("evicted permit changed incompatible resource: before=%+v after=%+v", before, after)
	}
	if after.ResourceSlot > 2 {
		t.Fatalf("new ResourceSlot = %d, want reuse within bounded cardinality", after.ResourceSlot)
	}
	if snapshots := throttler.Snapshots(); len(snapshots) != 2 || snapshots[0].ResourceSlot >= snapshots[1].ResourceSlot {
		t.Fatalf("Snapshots() = %+v, want two snapshots ordered by stable slot", snapshots)
	}
	if !throttler.Reset("retained") {
		t.Fatal("Reset(retained) = false, want true")
	}
	if _, ok := throttler.Snapshot("retained"); ok {
		t.Fatal("Reset(retained) left history behind")
	}
}

func TestReusedSlotCannotReviveAnEvictedGeneration(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "generation-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 2},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
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
	old, err := throttler.TryAcquire(context.Background(), "same")
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	_ = throttler.Record("other", throttle.Classification{Outcome: throttle.Accepted})
	_ = throttler.Record("same", throttle.Classification{Outcome: throttle.DownstreamOverload})
	before, _ := throttler.Snapshot("same")
	if err := old.Record(throttle.Classification{Outcome: throttle.Accepted}); err != nil {
		t.Fatalf("old Permit.Record() error = %v", err)
	}
	after, _ := throttler.Snapshot("same")
	if after != before {
		t.Fatalf("old generation merged after slot reuse: before=%+v after=%+v", before, after)
	}
}
