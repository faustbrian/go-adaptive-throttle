package throttle_test

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

type anomalousRandom struct {
	value float64
	panic bool
}

func (r anomalousRandom) Float64() float64 {
	if r.panic {
		panic("random failure")
	}
	return r.value
}

func newTestThrottler(t *testing.T, random throttle.Random) *throttle.Throttler {
	t.Helper()
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "boundary-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 2},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.6,
		MinimumAdmissionProbability: 0.2,
		MaxResources:                2,
		Clock:                       &fixedClock{now: time.Unix(1_700_000_000, 0)},
		Random:                      random,
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return throttler
}

func TestProbabilityCapPreservesProbeFlowAndRandomAnomaliesAdmit(t *testing.T) {
	t.Parallel()

	for name, random := range map[string]throttle.Random{
		"NaN":      anomalousRandom{value: math.NaN()},
		"negative": anomalousRandom{value: -1},
		"unit":     anomalousRandom{value: 1},
		"panic":    anomalousRandom{panic: true},
	} {
		t.Run(name, func(t *testing.T) {
			throttler := newTestThrottler(t, random)
			if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
				t.Fatalf("Record(overload) error = %v", err)
			}
			for range 100 {
				if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.LocalRejection}); err != nil {
					t.Fatalf("Record(local rejection) error = %v", err)
				}
			}
			snapshot, _ := throttler.Snapshot("inventory")
			if snapshot.RejectionProbability < 0 || snapshot.RejectionProbability >= 0.6 || !isFinite(snapshot.RejectionProbability) {
				t.Fatalf("RejectionProbability = %v, want finite value in [0, 0.6)", snapshot.RejectionProbability)
			}
			permit, err := throttler.TryAcquire(context.Background(), "inventory")
			if err != nil || permit == nil {
				t.Fatalf("TryAcquire() = (%v, %v), random anomaly must fail open", permit, err)
			}
		})
	}
}

func TestFixedRandomStreamMakesExactProbabilityBoundaryDecisions(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		sample     float64
		wantReject bool
	}{
		{name: "immediately below", sample: math.Nextafter(0.5, 0), wantReject: true},
		{name: "equal", sample: 0.5, wantReject: false},
		{name: "immediately above", sample: math.Nextafter(0.5, 1), wantReject: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			throttler := newTestThrottler(t, fixedRandom{value: test.sample})
			if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
				t.Fatalf("Record() error = %v", err)
			}
			permit, err := throttler.TryAcquire(context.Background(), "inventory")
			if test.wantReject {
				if !errors.Is(err, throttle.ErrRejected) || permit != nil {
					t.Fatalf("TryAcquire() = (%v, %v), sample below probability must reject", permit, err)
				}
				return
			}
			if err != nil || permit == nil {
				t.Fatalf("TryAcquire() = (%v, %v), sample at or above probability must admit", permit, err)
			}
		})
	}
}

func TestAPIBoundariesFailWithoutContaminatingHistory(t *testing.T) {
	t.Parallel()

	throttler := newTestThrottler(t, fixedRandom{value: 0.99})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := throttler.TryAcquire(canceled, "canceled"); !errors.Is(err, context.Canceled) {
		t.Fatalf("TryAcquire(canceled) error = %v", err)
	}
	if _, ok := throttler.Snapshot("canceled"); ok {
		t.Fatal("canceled admission created history")
	}
	if _, err := throttler.TryAcquire(nilTestContext(), "resource"); err == nil {
		t.Fatal("TryAcquire(nil context) error = nil")
	}
	if _, err := throttler.TryAcquire(context.Background(), ""); err == nil {
		t.Fatal("TryAcquire(empty resource) error = nil")
	}
	if err := throttler.Record(strings.Repeat("x", throttle.MaxResourceBytes+1), throttle.Classification{Outcome: throttle.Accepted}); err == nil {
		t.Fatal("Record(long resource) error = nil")
	}
	if err := throttler.Record("resource", throttle.Classification{Outcome: throttle.Outcome(255)}); err == nil {
		t.Fatal("Record(invalid outcome) error = nil")
	}
	if _, ok := throttler.Snapshot(""); ok {
		t.Fatal("Snapshot(invalid resource) = found")
	}
	if throttler.Reset("") || throttler.Reset("missing") {
		t.Fatal("Reset(invalid or missing) = true")
	}
	if count := throttler.ResetAll(); count != 0 {
		t.Fatalf("ResetAll() = %d, want 0", count)
	}
	if _, err := throttle.Execute[struct{}](context.Background(), nil, "resource", func(context.Context) (struct{}, error) { return struct{}{}, nil }); err == nil {
		t.Fatal("Execute(nil throttler) error = nil")
	}
	if _, err := throttle.Execute[struct{}](context.Background(), throttler, "resource", nil); err == nil {
		t.Fatal("Execute(nil operation) error = nil")
	}
	var permit *throttle.Permit
	if err := permit.Record(throttle.Classification{Outcome: throttle.Accepted}); err == nil {
		t.Fatal("nil Permit.Record() error = nil")
	}
	permit, err := throttler.TryAcquire(context.Background(), "resource")
	if err != nil {
		t.Fatalf("TryAcquire() error = %v", err)
	}
	if err := permit.Record(throttle.Classification{Outcome: throttle.LocalRejection}); err == nil {
		t.Fatal("Permit.Record(local rejection) error = nil")
	}
	if err := permit.Record(throttle.Classification{Outcome: throttle.Accepted}); err != nil {
		t.Fatalf("Permit.Record() error = %v", err)
	}
	if err := permit.Record(throttle.Classification{Outcome: throttle.Accepted}); err == nil {
		t.Fatal("second Permit.Record() error = nil")
	}
	if count := throttler.ResetAll(); count != 1 {
		t.Fatalf("ResetAll() = %d, want 1", count)
	}
}

func isFinite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }

func nilTestContext() context.Context { return nil }
