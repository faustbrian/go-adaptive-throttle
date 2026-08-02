package throttle_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

type raceClock struct{ nanoseconds atomic.Int64 }

func (c *raceClock) Now() time.Time { return time.Unix(0, c.nanoseconds.Load()) }

func TestConcurrentAdmissionRecordSnapshotResetAndEviction(t *testing.T) {
	t.Parallel()

	clock := &raceClock{}
	clock.nanoseconds.Store(time.Unix(1_700_000_000, 0).UnixNano())
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "race-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 4},
		MinimumSamples:              2,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                4,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0.5},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var workers sync.WaitGroup
	for worker := range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			resource := string(rune('a' + worker%8))
			for iteration := range 250 {
				ctx := context.Background()
				if iteration%17 == 0 {
					canceled, cancel := context.WithCancel(ctx)
					cancel()
					ctx = canceled
				}
				permit, acquireErr := throttler.TryAcquire(ctx, resource)
				if acquireErr == nil {
					outcome := throttle.Accepted
					if iteration%7 == 0 {
						outcome = throttle.DownstreamOverload
					}
					if recordErr := permit.Record(throttle.Classification{Outcome: outcome}); recordErr != nil {
						t.Errorf("Permit.Record() error = %v", recordErr)
					}
				}
				_ = throttler.Snapshots()
				if iteration%101 == 0 {
					throttler.Reset(resource)
				}
				if iteration%113 == 0 {
					throttler.ResetAll()
				}
				clock.nanoseconds.Add(int64(10 * time.Millisecond))
			}
		}()
	}
	workers.Wait()
	if snapshots := throttler.Snapshots(); len(snapshots) > 4 {
		t.Fatalf("retained snapshots = %d, want at most 4", len(snapshots))
	}
}
