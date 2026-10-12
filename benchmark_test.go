package throttle

import (
	"context"
	"testing"
	"time"

	"github.com/failsafe-go/failsafe-go/adaptivethrottler"
)

type benchmarkClock struct{ now time.Time }

func (c benchmarkClock) Now() time.Time { return c.now }

type benchmarkRandom struct{ value float64 }

func (r benchmarkRandom) Float64() float64 { return r.value }

func benchmarkThrottler(b *testing.B, observer Observer) *Throttler {
	b.Helper()
	policy, err := NewPolicy(PolicyConfig{
		Revision:                    "benchmark-v1",
		Window:                      WindowConfig{BucketDuration: time.Second, BucketCount: 120},
		MinimumSamples:              10,
		Algorithm:                   GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       benchmarkClock{now: time.Unix(1_700_000_000, 0)},
		Random:                      benchmarkRandom{value: 0.5},
		Observer:                    observer,
	})
	if err != nil {
		b.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := New(policy)
	if err != nil {
		b.Fatalf("New() error = %v", err)
	}
	return throttler
}

// BenchmarkTryAcquireAndRecordHealthy measures healthy admission plus
// completion for one frozen-clock resource with 120 buckets.
// Policy/throttler/context setup is excluded by ResetTimer; the first lazy
// history allocation remains in the loop.
func BenchmarkTryAcquireAndRecordHealthy(b *testing.B) {
	throttler := benchmarkThrottler(b, nil)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		permit, err := throttler.TryAcquire(ctx, "backend")
		if err != nil {
			b.Fatal(err)
		}
		if err := permit.Record(Classification{Outcome: Accepted}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTryAcquireAndRecordWithObserver measures the same healthy
// admission/completion boundary with a synchronous no-op observer. ResetTimer
// excludes construction; it isolates dispatch overhead, not realistic observer
// work.
func BenchmarkTryAcquireAndRecordWithObserver(b *testing.B) {
	throttler := benchmarkThrottler(b, func(Event) {})
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		permit, err := throttler.TryAcquire(ctx, "backend")
		if err != nil {
			b.Fatal(err)
		}
		if err := permit.Record(Classification{Outcome: Accepted}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGoogleSREEquation measures only the equation/cap calculation for
// fixed 1000 requests, 400 accepts and 800 samples. Fixture setup precedes
// b.Loop; no locking, history allocation or collaborator dispatch is measured.
func BenchmarkGoogleSREEquation(b *testing.B) {
	policy := policyConfig{minimumSamples: 10, acceptsK: 2, maxProbability: 0.9}
	snapshot := Snapshot{Requests: 1_000, Accepts: 400, Samples: 800}
	b.ReportAllocs()
	for b.Loop() {
		_ = rejectionProbability(snapshot, policy)
	}
}

// BenchmarkFailsafeGoEquivalentHealthy measures Failsafe-Go healthy permit
// acquisition plus success recording after builder setup is excluded by
// ResetTimer. The threshold/minimum/window/cap align with our healthy case, but
// clocks, ownership and callback policies differ; it is not rejection-feedback
// parity.
func BenchmarkFailsafeGoEquivalentHealthy(b *testing.B) {
	throttler := adaptivethrottler.NewBuilder[struct{}]().
		WithFailureRateThreshold(0.5, 10, 2*time.Minute).
		WithMaxRejectionRate(0.9).
		Build()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if !throttler.TryAcquirePermit() {
			b.Fatal("healthy Failsafe-Go throttler rejected")
		}
		throttler.RecordSuccess()
	}
}

// BenchmarkEquivalentHealthyContention compares parallel healthy
// acquisition/completion for one resource with equivalent K/sample/window/cap
// settings. Construction precedes RunParallel but has no explicit timer reset,
// so one-time setup remains included. Our clock is fixed while Failsafe-Go uses
// wall time; only healthy-path contention is compared.
func BenchmarkEquivalentHealthyContention(b *testing.B) {
	b.Run("adaptive-throttle", func(b *testing.B) {
		throttler := benchmarkThrottler(b, nil)
		ctx := context.Background()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				permit, err := throttler.TryAcquire(ctx, "backend")
				if err != nil {
					b.Error(err)
					return
				}
				if err := permit.Record(Classification{Outcome: Accepted}); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
	b.Run("failsafe-go", func(b *testing.B) {
		throttler := adaptivethrottler.NewBuilder[struct{}]().
			WithFailureRateThreshold(0.5, 10, 2*time.Minute).
			WithMaxRejectionRate(0.9).
			Build()
		b.ReportAllocs()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if !throttler.TryAcquirePermit() {
					b.Error("healthy Failsafe-Go throttler rejected")
					return
				}
				throttler.RecordSuccess()
			}
		})
	})
}
