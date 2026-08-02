package throttle

import (
	"context"
	"math"
	"testing"
	"time"
)

func FuzzGoogleSREProbabilityFiniteAndBounded(f *testing.F) {
	f.Add(uint64(8), uint64(3), uint64(8), uint16(20), uint16(800))
	f.Add(uint64(math.MaxUint64), uint64(0), uint64(math.MaxUint64), uint16(10), uint16(999))
	f.Fuzz(func(t *testing.T, requests, accepts, samples uint64, multiplierInput, maximumInput uint16) {
		multiplier := 1 + float64(multiplierInput%10_000)/10
		maximum := 0.001 + float64(maximumInput%998)/1_000
		policy := policyConfig{minimumSamples: 1, acceptsK: multiplier, maxProbability: maximum}
		probability := rejectionProbability(Snapshot{Requests: requests, Accepts: accepts, Samples: samples}, policy)
		if !finite(probability) || probability < 0 || probability >= maximum {
			t.Fatalf("probability = %v, maximum = %v", probability, maximum)
		}
	})
}

type fuzzClock struct{ now time.Time }

func (c *fuzzClock) Now() time.Time { return c.now }

type fuzzRandom struct{ value float64 }

func (r fuzzRandom) Float64() float64 { return r.value }

func FuzzBoundedEventSequences(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6})
	f.Add([]byte{255, 255, 0, 4})
	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 4_096 {
			t.Skip()
		}
		clock := &fuzzClock{now: time.Unix(0, 0)}
		policy, err := NewPolicy(PolicyConfig{
			Revision:                    "fuzz-v1",
			Window:                      WindowConfig{BucketDuration: time.Millisecond, BucketCount: 4},
			MinimumSamples:              1,
			Algorithm:                   GoogleSRE{AcceptMultiplier: 2},
			MaxRejectionProbability:     0.9,
			MinimumAdmissionProbability: 0.1,
			MaxResources:                4,
			Clock:                       clock,
			Random:                      fuzzRandom{value: 0.5},
		})
		if err != nil {
			t.Fatalf("NewPolicy() error = %v", err)
		}
		throttler, err := New(policy)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		for index, operation := range operations {
			resource := string(rune('a' + operation%8))
			switch operation % 7 {
			case 0:
				_ = throttler.Record(resource, Classification{Outcome: Accepted})
			case 1:
				_ = throttler.Record(resource, Classification{Outcome: DownstreamOverload})
			case 2:
				_ = throttler.Record(resource, Classification{Outcome: DownstreamFailure})
			case 3:
				permit, acquireErr := throttler.TryAcquire(context.Background(), resource)
				if acquireErr == nil {
					_ = permit.Record(Classification{Outcome: Accepted})
				}
			case 4:
				throttler.Reset(resource)
			case 5:
				_ = throttler.Snapshots()
			case 6:
				clock.now = clock.now.Add(time.Duration(int8(operation)) * time.Millisecond)
			}
			if index%32 == 0 {
				for _, snapshot := range throttler.Snapshots() {
					if !finite(snapshot.RejectionProbability) || snapshot.RejectionProbability < 0 || snapshot.RejectionProbability >= 0.9 {
						t.Fatalf("invalid snapshot: %+v", snapshot)
					}
				}
			}
		}
		if snapshots := throttler.Snapshots(); len(snapshots) > 4 {
			t.Fatalf("retained resources = %d, want at most 4", len(snapshots))
		}
	})
}

func FuzzBucketIndexBounded(f *testing.F) {
	f.Add(int64(-1), uint16(3))
	f.Add(int64(math.MaxInt64), uint16(MaxBuckets))
	f.Fuzz(func(t *testing.T, tick int64, countInput uint16) {
		count := 1 + int(countInput%MaxBuckets)
		index := bucketIndex(tick, count)
		if index < 0 || index >= count {
			t.Fatalf("bucketIndex(%d, %d) = %d", tick, count, index)
		}
	})
}
