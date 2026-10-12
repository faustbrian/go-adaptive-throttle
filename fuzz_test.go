package throttle

import (
	"context"
	"math"
	"testing"
	"time"
)

// FuzzGoogleSREProbabilityFiniteAndBounded varies full uint64 counters with
// bounded multiplier/cap inputs. The equation must remain finite, nonnegative
// and strictly below its effective cap, including inconsistent synthetic
// histories and saturated values.
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

// FuzzBoundedEventSequences models at most 4096
// record/acquire/reset/read/time-change operations over eight keys and four
// retained histories. Periodic snapshots must have finite bounded probability,
// and cardinality must remain at most four; this is not a downstream-capacity
// model.
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

// FuzzBucketIndexBounded varies all signed ticks with bucket counts in [1,
// MaxBuckets]; modulo normalization must always produce a valid ring index,
// including pre-epoch ticks.
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

// FuzzPolicyConfigurationRemainsBounded varies duration/count/capacity and
// floating-point bit patterns. Rejected configurations are expected; accepted
// policies must construct and record bounded finite state without overflow.
func FuzzPolicyConfigurationRemainsBounded(f *testing.F) {
	f.Add(int64(time.Second), int16(4), uint64(10), math.Float64bits(2), math.Float64bits(0.9), math.Float64bits(0.1), int32(4))
	f.Add(int64(math.MaxInt64), int16(-1), uint64(math.MaxUint64), math.Float64bits(math.NaN()), math.Float64bits(math.Inf(1)), math.Float64bits(-1), int32(math.MaxInt32))
	f.Fuzz(func(t *testing.T, duration int64, bucketCount int16, minimumSamples, multiplierBits, maximumBits, admissionBits uint64, maxResources int32) {
		policy, err := NewPolicy(PolicyConfig{
			Revision:                    "configuration-fuzz-v1",
			Window:                      WindowConfig{BucketDuration: time.Duration(duration), BucketCount: int(bucketCount)},
			MinimumSamples:              minimumSamples,
			Algorithm:                   GoogleSRE{AcceptMultiplier: math.Float64frombits(multiplierBits)},
			MaxRejectionProbability:     math.Float64frombits(maximumBits),
			MinimumAdmissionProbability: math.Float64frombits(admissionBits),
			MaxResources:                int(maxResources),
			Clock:                       &fuzzClock{now: time.Unix(0, 0)},
			Random:                      fuzzRandom{value: 0.5},
		})
		if err != nil {
			return
		}
		throttler, err := New(policy)
		if err != nil {
			t.Fatalf("New(valid policy) error = %v", err)
		}
		if err := throttler.Record("bounded", Classification{Outcome: DownstreamOverload}); err != nil {
			t.Fatalf("Record() error = %v", err)
		}
		snapshot, ok := throttler.Snapshot("bounded")
		if !ok || !finite(snapshot.RejectionProbability) || snapshot.RejectionProbability < 0 || snapshot.RejectionProbability >= 1 {
			t.Fatalf("Snapshot() = %+v, want finite bounded state", snapshot)
		}
	})
}

// FuzzCounterSaturationMatchesReference varies full uint64 addends and compares
// the production counter sum with an independent overflow-to-MaxUint64
// reference.
func FuzzCounterSaturationMatchesReference(f *testing.F) {
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64-1), uint64(2))
	f.Fuzz(func(t *testing.T, left, right uint64) {
		got := left
		saturatingAdd(&got, right)
		want := left + right
		if want < left {
			want = math.MaxUint64
		}
		if got != want {
			t.Fatalf("saturatingAdd(%d, %d) = %d, want %d", left, right, got, want)
		}
	})
}

// FuzzClassifierOutcomesFailSafely varies outcome/reason bytes and classifier
// panics. Invalid or local-rejection outcomes must become Ignored; reason bytes
// alone are not normalized or validated by this oracle.
func FuzzClassifierOutcomesFailSafely(f *testing.F) {
	f.Add(byte(Accepted), byte(ReasonSuccess), false)
	f.Add(byte(LocalRejection), byte(ReasonLocalPolicy), false)
	f.Add(byte(255), byte(255), false)
	f.Add(byte(DownstreamOverload), byte(ReasonExplicitOverload), true)
	f.Fuzz(func(t *testing.T, outcome, reason byte, panicClassifier bool) {
		classification := safeClassify(func(Completion) Classification {
			if panicClassifier {
				panic("classifier fault")
			}
			return Classification{Outcome: Outcome(outcome), Reason: Reason(reason)}
		}, Completion{})
		if classification.Outcome > Ignored {
			t.Fatalf("safeClassify() = %+v, admitted classifier produced unsafe outcome", classification)
		}
		if panicClassifier || Outcome(outcome) == LocalRejection || Outcome(outcome) > LocalRejection {
			if classification.Outcome != Ignored {
				t.Fatalf("safeClassify() = %+v, want ignored fallback", classification)
			}
		}
	})
}
