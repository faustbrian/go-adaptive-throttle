package throttle_test

import (
	"context"
	"sync"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

func TestDryRunObservesWouldRejectOutsideLock(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	var throttler *throttle.Throttler
	var mu sync.Mutex
	var events []throttle.Event
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "dry-run-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 10},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0},
		DryRun:                      true,
		Observer: func(event throttle.Event) {
			// Re-entry proves the callback is not invoked under the state lock.
			if _, ok := throttler.Snapshot("inventory"); !ok {
				t.Error("observer could not inspect resource snapshot")
			}
			mu.Lock()
			events = append(events, event)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err = throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.DownstreamOverload, Reason: throttle.ReasonExplicitOverload}); err != nil {
		t.Fatalf("Record() error = %v", err)
	}

	permit, err := throttler.TryAcquire(context.Background(), "inventory")
	if err != nil || permit == nil {
		t.Fatalf("TryAcquire() = (%v, %v), want admitted dry-run permit", permit, err)
	}
	snapshot, _ := throttler.Snapshot("inventory")
	if snapshot.LocalRejections != 0 || snapshot.DryRunRejections != 1 {
		t.Fatalf("Snapshot() local = %d, dry-run = %d, want 0 and 1", snapshot.LocalRejections, snapshot.DryRunRejections)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) == 0 {
		t.Fatal("observer received no events")
	}
	last := events[len(events)-1]
	if last.Decision != throttle.DecisionDryRunAdmit || last.Probability <= 0 || last.ResourceSlot == 0 {
		t.Fatalf("last Event = %+v, want bounded dry-run decision", last)
	}
}

func TestResetEventExpiresStaleHistory(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	var last throttle.Event
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "reset-event-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 2},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0.99},
		Observer:                    func(event throttle.Event) { last = event },
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
	clock.now = clock.now.Add(2 * time.Second)
	if !throttler.Reset("inventory") {
		t.Fatal("Reset() = false")
	}
	if last.Decision != throttle.DecisionReset || last.Snapshot.Requests != 0 {
		t.Fatalf("reset Event = %+v, want expired snapshot", last)
	}
}
