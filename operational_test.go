package throttle_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

type deterministicStream struct{ state uint64 }

func (r *deterministicStream) Float64() float64 {
	r.state ^= r.state << 13
	r.state ^= r.state >> 7
	r.state ^= r.state << 17
	return float64(r.state>>11) / (1 << 53)
}

type simulationMetrics struct {
	offered         int
	admitted        int
	goodput         int
	downstreamFails int
	rejected        int
	recoveryOffers  int
	peakProbability float64
}

func TestDeterministicAdmissionGoodputAndRecoverySimulation(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	stream := &deterministicStream{state: 0xd1b54a32d192ed03}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "quality-simulation-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 120},
		MinimumSamples:              20,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      stream,
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	metrics := simulationMetrics{}
	run := func(overloaded bool) {
		metrics.offered++
		permit, acquireErr := throttler.TryAcquire(context.Background(), "backend")
		if errors.Is(acquireErr, throttle.ErrRejected) {
			metrics.rejected++
			return
		}
		if acquireErr != nil {
			t.Fatalf("TryAcquire() error = %v", acquireErr)
		}
		metrics.admitted++
		outcome := throttle.Accepted
		if overloaded {
			outcome = throttle.DownstreamOverload
			metrics.downstreamFails++
		} else {
			metrics.goodput++
		}
		if err := permit.Record(throttle.Classification{Outcome: outcome}); err != nil {
			t.Fatalf("Permit.Record() error = %v", err)
		}
		snapshot, _ := throttler.Snapshot("backend")
		metrics.peakProbability = max(metrics.peakProbability, snapshot.RejectionProbability)
	}
	for range 200 {
		run(false)
	}
	for range 400 {
		run(true)
	}
	for metrics.recoveryOffers < 2_000 {
		metrics.recoveryOffers++
		run(false)
		snapshot, _ := throttler.Snapshot("backend")
		if snapshot.RejectionProbability == 0 {
			break
		}
	}
	final, _ := throttler.Snapshot("backend")
	if metrics.goodput < 200 || metrics.downstreamFails == 0 || metrics.rejected == 0 ||
		metrics.peakProbability <= 0 || metrics.peakProbability >= 0.9 || metrics.recoveryOffers >= 2_000 ||
		final.RejectionProbability != 0 {
		t.Fatalf("simulation metrics = %+v final = %+v", metrics, final)
	}
	t.Logf("offered=%d admitted=%d goodput=%d downstream_overload=%d rejected=%d recovery_offers=%d peak_probability=%.6f",
		metrics.offered, metrics.admitted, metrics.goodput, metrics.downstreamFails,
		metrics.rejected, metrics.recoveryOffers, metrics.peakProbability)
}

func TestFixedSeedStatisticalRejectionMatchesJustifiedConfidenceBound(t *testing.T) {
	t.Parallel()

	const (
		draws = 100_000
		alpha = 1e-9
	)
	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	stream := &deterministicStream{state: 0x9e3779b97f4a7c15}
	wouldReject := 0
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "statistical-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 4},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      stream,
		DryRun:                      true,
		Observer: func(event throttle.Event) {
			if event.Decision == throttle.DecisionDryRunAdmit {
				wouldReject++
			}
		},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	for range 3 {
		_ = throttler.Record("backend", throttle.Classification{Outcome: throttle.Accepted})
	}
	for range 5 {
		_ = throttler.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload})
	}
	before, _ := throttler.Snapshot("backend")
	for range draws {
		permit, acquireErr := throttler.TryAcquire(context.Background(), "backend")
		if acquireErr != nil || permit == nil {
			t.Fatalf("dry-run TryAcquire() = (%v, %v), want admission", permit, acquireErr)
		}
	}
	after, _ := throttler.Snapshot("backend")
	observed := float64(wouldReject) / draws
	epsilon := math.Sqrt(math.Log(2/alpha) / (2 * draws))
	if math.Abs(observed-before.RejectionProbability) > epsilon {
		t.Fatalf("fixed-seed rejection rate = %.6f, probability = %.6f, Hoeffding epsilon = %.6f", observed, before.RejectionProbability, epsilon)
	}
	if after.Requests != before.Requests || after.Samples != before.Samples || after.DryRunRejections != uint64(wouldReject) {
		t.Fatalf("dry-run Snapshot() = %+v, before = %+v", after, before)
	}
	t.Logf("draws=%d expected=%.6f observed=%.6f epsilon=%.6f alpha=%g", draws, before.RejectionProbability, observed, epsilon, alpha)
}

func TestRetryStopsAtLocalRejectionWithoutFeedbackAmplification(t *testing.T) {
	t.Parallel()

	throttler := newTestThrottler(t, fixedRandom{value: 0})
	if err := throttler.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	operationCalls := 0
	attempts := 0
	for attempts < 5 {
		attempts++
		_, err := throttle.Execute(context.Background(), throttler, "backend", func(context.Context) (struct{}, error) {
			operationCalls++
			return struct{}{}, nil
		})
		if errors.Is(err, throttle.ErrRejected) {
			break
		}
	}
	if attempts != 1 || operationCalls != 0 {
		t.Fatalf("retry attempts = %d, operation calls = %d, want terminal local rejection", attempts, operationCalls)
	}
	snapshot, _ := throttler.Snapshot("backend")
	if snapshot.Samples != 1 || snapshot.Overloads != 1 || snapshot.LocalRejections != 1 {
		t.Fatalf("Snapshot() = %+v, local retry rejection contaminated downstream samples", snapshot)
	}
}

func TestMisleadingHPASignalAndWeightedFleetAdmissionModel(t *testing.T) {
	t.Parallel()

	beforeDemand, beforeProbability := 100.0, 0.0
	afterDemand, afterProbability := 200.0, 0.8
	beforeCPU := beforeDemand * (1 - beforeProbability)
	afterCPU := afterDemand * (1 - afterProbability)
	if afterDemand <= beforeDemand || afterCPU >= beforeCPU {
		t.Fatalf("HPA model demand %.0f->%.0f CPU %.0f->%.0f does not expose inverse signal", beforeDemand, afterDemand, beforeCPU, afterCPU)
	}

	rates := []float64{100, 20, 5}
	probabilities := []float64{0.1, 0.5, 0.9}
	weighted := 0.0
	for index := range rates {
		weighted += rates[index] * (1 - probabilities[index])
	}
	meanProbability := (probabilities[0] + probabilities[1] + probabilities[2]) / 3
	naive := (rates[0] + rates[1] + rates[2]) * (1 - meanProbability)
	if weighted != 100.5 || naive == weighted {
		t.Fatalf("fleet admission weighted=%v naive-average=%v", weighted, naive)
	}
}
