package throttle_test

import (
	"math"
	"testing"
	"time"

	"github.com/failsafe-go/failsafe-go/adaptivethrottler"
	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

func TestEquivalentFailsafeGoPolicyMatchesGoogleSREProbabilityGrid(t *testing.T) {
	t.Parallel()

	const maximum = 0.99
	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	maximumError := 0.0
	comparisons := 0
	for _, acceptsK := range []float64{1, 1.25, 2, 4} {
		failureRateThreshold := 1 - 1/acceptsK
		for _, minimumSamples := range []uint64{1, 5, 20} {
			for accepts := range 21 {
				for overloads := range 21 {
					policy, err := throttle.NewPolicy(throttle.PolicyConfig{
						Revision:                    "failsafe-comparison-v1",
						Window:                      throttle.WindowConfig{BucketDuration: 3 * time.Second, BucketCount: 20},
						MinimumSamples:              minimumSamples,
						Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: acceptsK},
						MaxRejectionProbability:     maximum,
						MinimumAdmissionProbability: 0.01,
						MaxResources:                1,
						Clock:                       clock,
						Random:                      fixedRandom{value: math.Nextafter(1, 0)},
					})
					if err != nil {
						t.Fatalf("NewPolicy() error = %v", err)
					}
					ours, err := throttle.New(policy)
					if err != nil {
						t.Fatalf("New() error = %v", err)
					}
					failsafe := adaptivethrottler.NewBuilder[struct{}]().
						WithFailureRateThreshold(failureRateThreshold, uint(minimumSamples), time.Minute).
						WithMaxRejectionRate(maximum).
						Build()

					for range accepts {
						if err := ours.Record("backend", throttle.Classification{Outcome: throttle.Accepted}); err != nil {
							t.Fatalf("Record(accepted) error = %v", err)
						}
						failsafe.RecordSuccess()
					}
					for range overloads {
						if err := ours.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
							t.Fatalf("Record(overload) error = %v", err)
						}
						failsafe.RecordFailure()
					}
					snapshot, _ := ours.Snapshot("backend")
					_ = failsafe.TryAcquirePermit()
					error := math.Abs(snapshot.RejectionProbability - failsafe.RejectionRate())
					maximumError = max(maximumError, error)
					comparisons++
					if error > 1e-14 {
						t.Fatalf("K=%v minimum=%d accepts=%d overloads=%d probability error=%g: ours=%g failsafe=%g",
							acceptsK, minimumSamples, accepts, overloads, error,
							snapshot.RejectionProbability, failsafe.RejectionRate())
					}
				}
			}
		}
	}
	t.Logf("compared %d aligned probability states; maximum absolute error %.3g", comparisons, maximumError)
}

func TestEquivalentFailsafeGoTrafficPhasesMatchUntilLocalRejection(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "failsafe-phases-v1",
		Window:                      throttle.WindowConfig{BucketDuration: 3 * time.Second, BucketCount: 20},
		MinimumSamples:              20,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      fixedRandom{value: math.Nextafter(1, 0)},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	ours, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	failsafe := adaptivethrottler.NewBuilder[struct{}]().
		WithFailureRateThreshold(0.5, 20, time.Minute).
		WithMaxRejectionRate(0.9).
		Build()
	step := 0
	peak := 0.0
	record := func(overloaded bool) {
		t.Helper()
		step++
		outcome := throttle.Accepted
		if overloaded {
			outcome = throttle.DownstreamOverload
			failsafe.RecordFailure()
		} else {
			failsafe.RecordSuccess()
		}
		if err := ours.Record("backend", throttle.Classification{Outcome: outcome}); err != nil {
			t.Fatalf("step %d Record() error = %v", step, err)
		}
		_ = failsafe.TryAcquirePermit()
		snapshot, _ := ours.Snapshot("backend")
		if snapshot.RejectionProbability != failsafe.RejectionRate() {
			t.Fatalf("step %d probability ours=%g failsafe=%g", step, snapshot.RejectionProbability, failsafe.RejectionRate())
		}
		peak = max(peak, snapshot.RejectionProbability)
	}
	for range 40 {
		record(false)
	}
	for index := range 20 {
		record(index%2 == 0)
	}
	for range 80 {
		record(true)
	}
	for range 100 {
		record(false)
	}
	final, _ := ours.Snapshot("backend")
	if peak <= 0 || final.RejectionProbability != 0 {
		t.Fatalf("traffic phase peak=%g final=%+v", peak, final)
	}
	t.Logf("compared %d healthy/partial/outage/recovery states; peak probability %.6f", step, peak)
}
