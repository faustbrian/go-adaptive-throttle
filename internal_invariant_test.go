package throttle

import (
	"math"
	"testing"
	"time"
)

func TestForwardGapHandlesFullInt64Domain(t *testing.T) {
	t.Parallel()

	if !forwardGapAtLeast(math.MinInt64, math.MaxInt64, 2) {
		t.Fatal("forwardGapAtLeast() = false across full int64 domain")
	}
	if forwardGapAtLeast(10, 11, 2) {
		t.Fatal("forwardGapAtLeast() = true for one tick")
	}
}

func TestInternalIdentityCountersRemainBoundedAtSaturation(t *testing.T) {
	t.Parallel()

	throttler := &Throttler{
		policy:    policyConfig{bucketDuration: time.Second, bucketCount: 1, maxResources: 1},
		resources: make(map[string]*resourceState),
		sequence:  math.MaxUint64,
		slots:     make([]bool, 2),
	}
	state := throttler.resourceLocked("resource", time.Unix(0, 0))
	if state.lastUsed != math.MaxUint64 || state.slot != 1 {
		t.Fatalf("resource state = %+v", state)
	}
	throttler.slots[0] = true
	if slot := throttler.availableSlotLocked(); slot != 0 {
		t.Fatalf("availableSlotLocked() = %d, want bounded sentinel zero", slot)
	}
}

func TestInternalIdentitySequenceAdvancesNormally(t *testing.T) {
	t.Parallel()

	throttler := &Throttler{
		policy:    policyConfig{bucketDuration: time.Second, bucketCount: 1, maxResources: 1},
		resources: make(map[string]*resourceState),
		slots:     make([]bool, 2),
	}
	state := throttler.resourceLocked("resource", time.Unix(0, 0))
	if throttler.sequence != 1 || state.lastUsed != 1 {
		t.Fatalf("sequence = %d, lastUsed = %d", throttler.sequence, state.lastUsed)
	}
}
