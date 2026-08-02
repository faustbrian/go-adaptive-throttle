package throttle_test

import (
	"math"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

type referenceCounts struct {
	requests        uint64
	accepts         uint64
	samples         uint64
	overloads       uint64
	failures        uint64
	ignored         uint64
	localRejections uint64
}

type rollingReference struct {
	bucketDuration time.Duration
	bucketCount    int64
	minimumSamples uint64
	acceptsK       float64
	maximum        float64
	lastTime       time.Time
	buckets        map[int64]referenceCounts
}

func (r *rollingReference) record(now time.Time, outcome throttle.Outcome) throttle.Snapshot {
	tick := now.UnixNano() / int64(r.bucketDuration)
	if !r.lastTime.IsZero() {
		lastTick := r.lastTime.UnixNano() / int64(r.bucketDuration)
		if now.Before(r.lastTime) || tick-lastTick >= r.bucketCount {
			clear(r.buckets)
		}
	}
	for retained := range r.buckets {
		if tick-retained >= r.bucketCount {
			delete(r.buckets, retained)
		}
	}
	r.lastTime = now
	counts := r.buckets[tick]
	switch outcome {
	case throttle.Accepted:
		counts.requests++
		counts.accepts++
		counts.samples++
	case throttle.DownstreamOverload:
		counts.requests++
		counts.samples++
		counts.overloads++
	case throttle.DownstreamFailure:
		counts.requests++
		counts.accepts++
		counts.samples++
		counts.failures++
	case throttle.Ignored:
		counts.ignored++
	case throttle.LocalRejection:
		counts.requests++
		counts.localRejections++
	}
	r.buckets[tick] = counts

	snapshot := throttle.Snapshot{}
	oldest := tick
	for retained, bucket := range r.buckets {
		snapshot.Requests += bucket.requests
		snapshot.Accepts += bucket.accepts
		snapshot.Samples += bucket.samples
		snapshot.Overloads += bucket.overloads
		snapshot.Failures += bucket.failures
		snapshot.Ignored += bucket.ignored
		snapshot.LocalRejections += bucket.localRejections
		if retained < oldest {
			oldest = retained
		}
	}
	if len(r.buckets) > 0 {
		snapshot.WindowAge = time.Duration(tick-oldest) * r.bucketDuration
	}
	if snapshot.Samples >= r.minimumSamples && snapshot.Requests > 0 {
		probability := (float64(snapshot.Requests) - r.acceptsK*float64(snapshot.Accepts)) /
			(float64(snapshot.Requests) + 1)
		if probability > 0 {
			snapshot.RejectionProbability = min(probability, math.Nextafter(r.maximum, 0))
		}
	}
	return snapshot
}

func TestRollingWindowMatchesDeterministicReferenceModelAtEveryTransition(t *testing.T) {
	t.Parallel()

	const resource = "backend"
	base := time.Unix(1_700_000_000, 0)
	clock := &fixedClock{now: base}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "differential-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 4},
		MinimumSamples:              3,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1.5},
		MaxRejectionProbability:     0.75,
		MinimumAdmissionProbability: 0.2,
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
	reference := rollingReference{
		bucketDuration: time.Second,
		bucketCount:    4,
		minimumSamples: 3,
		acceptsK:       1.5,
		maximum:        0.75,
		buckets:        make(map[int64]referenceCounts),
	}
	steps := []struct {
		name    string
		at      time.Duration
		outcome throttle.Outcome
	}{
		{name: "first bucket accepted", at: 0, outcome: throttle.Accepted},
		{name: "same bucket overload", at: 500 * time.Millisecond, outcome: throttle.DownstreamOverload},
		{name: "next bucket failure", at: time.Second, outcome: throttle.DownstreamFailure},
		{name: "ignored bucket", at: 2 * time.Second, outcome: throttle.Ignored},
		{name: "local rejection bucket", at: 3 * time.Second, outcome: throttle.LocalRejection},
		{name: "exact oldest expiry and slot reuse", at: 4 * time.Second, outcome: throttle.DownstreamOverload},
		{name: "partial rollover", at: 6 * time.Second, outcome: throttle.DownstreamOverload},
		{name: "full forward reset", at: 10 * time.Second, outcome: throttle.Accepted},
		{name: "backward reset inside bucket", at: 9*time.Second + 900*time.Millisecond, outcome: throttle.DownstreamOverload},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			clock.now = base.Add(step.at)
			if err := throttler.Record(resource, throttle.Classification{Outcome: step.outcome}); err != nil {
				t.Fatalf("Record() error = %v", err)
			}
			got, ok := throttler.Snapshot(resource)
			if !ok {
				t.Fatal("Snapshot() did not retain resource")
			}
			want := reference.record(clock.now, step.outcome)
			assertReferenceSnapshot(t, got, want)
		})
	}
}

func assertReferenceSnapshot(t *testing.T, got, want throttle.Snapshot) {
	t.Helper()
	if got.Requests != want.Requests || got.Accepts != want.Accepts || got.Samples != want.Samples ||
		got.Overloads != want.Overloads || got.Failures != want.Failures || got.Ignored != want.Ignored ||
		got.LocalRejections != want.LocalRejections || got.DryRunRejections != want.DryRunRejections ||
		got.RejectionProbability != want.RejectionProbability || got.WindowAge != want.WindowAge {
		t.Fatalf("Snapshot() = %+v, want reference %+v", got, want)
	}
}
