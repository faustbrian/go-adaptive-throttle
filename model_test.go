package throttle

import (
	"math"
	"testing"
	"time"
)

func TestGoogleSREReferenceEquationBoundaries(t *testing.T) {
	t.Parallel()

	policy := policyConfig{minimumSamples: 2, acceptsK: 2, maxProbability: 0.8}
	tests := []struct {
		name     string
		snapshot Snapshot
		want     float64
	}{
		{name: "minimum samples", snapshot: Snapshot{Requests: 10, Samples: 1}, want: 0},
		{name: "no requests", snapshot: Snapshot{Samples: 2}, want: 0},
		{name: "healthy", snapshot: Snapshot{Requests: 10, Accepts: 10, Samples: 10}, want: 0},
		{name: "reference", snapshot: Snapshot{Requests: 8, Accepts: 3, Samples: 8}, want: 2.0 / 9.0},
		{name: "strict cap", snapshot: Snapshot{Requests: math.MaxUint64, Samples: 2}, want: math.Nextafter(0.8, 0)},
		{name: "large accepts", snapshot: Snapshot{Requests: math.MaxUint64, Accepts: math.MaxUint64, Samples: math.MaxUint64}, want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := rejectionProbability(test.snapshot, policy)
			if got != test.want || !finite(got) || got < 0 || got >= policy.maxProbability {
				t.Fatalf("rejectionProbability(%+v) = %.17g, want %.17g", test.snapshot, got, test.want)
			}
		})
	}
}

func TestCounterAndTimeArithmeticSaturatesSafely(t *testing.T) {
	t.Parallel()

	value := uint64(math.MaxUint64)
	increment(&value)
	if value != math.MaxUint64 {
		t.Fatalf("increment(max) = %d", value)
	}
	total := uint64(math.MaxUint64 - 1)
	saturatingAdd(&total, 2)
	if total != math.MaxUint64 {
		t.Fatalf("saturatingAdd() = %d", total)
	}
	negative := time.Unix(-1, 500_000_000)
	if tick := windowTick(negative, time.Second); tick != -1 {
		t.Fatalf("windowTick(-500ms) = %d, want -1", tick)
	}
	if index := bucketIndex(-1, 3); index != 2 {
		t.Fatalf("bucketIndex(-1, 3) = %d, want 2", index)
	}
}
