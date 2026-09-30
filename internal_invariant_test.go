package throttle

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

type admissionCheckpointContext struct {
	context.Context
	armed   bool
	checked chan struct{}
	resume  chan struct{}
}

func (ctx *admissionCheckpointContext) Err() error {
	err := ctx.Context.Err()
	if ctx.armed {
		ctx.armed = false
		close(ctx.checked)
		<-ctx.resume
	}
	return err
}

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

func TestCancellationAfterAdmissionCheckpointDoesNotCreateHistory(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &admissionCheckpointContext{Context: base, checked: make(chan struct{}), resume: make(chan struct{})}
	policy, err := NewPolicy(PolicyConfig{
		Revision:                    "lock-cancellation-v1",
		Window:                      WindowConfig{BucketDuration: time.Second, BucketCount: 1},
		MinimumSamples:              1,
		Algorithm:                   GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Priority: PriorityPolicy{RejectionScale: []float64{1}, Resolve: func(context.Context) Priority {
			ctx.armed = true
			return 0
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	throttler, err := New(policy)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	throttler.mu.Lock()
	go func() {
		_, acquireErr := throttler.TryAcquire(ctx, "inventory")
		result <- acquireErr
	}()
	<-ctx.checked
	cancel()
	close(ctx.resume)
	throttler.mu.Unlock()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("TryAcquire() error = %v, want context cancellation", err)
	}
	if _, ok := throttler.Snapshot("inventory"); ok {
		t.Fatal("cancellation while waiting for state lock created history")
	}
}
