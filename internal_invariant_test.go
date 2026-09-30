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
	armed      bool
	checked    chan struct{}
	resume     chan struct{}
	onCanceled func()
}

func (ctx *admissionCheckpointContext) Err() error {
	err := ctx.Context.Err()
	if ctx.armed {
		ctx.armed = false
		close(ctx.checked)
		<-ctx.resume
	}
	if err != nil && ctx.onCanceled != nil {
		ctx.onCanceled()
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
	done := make(chan struct{})
	locked, resumed := true, false
	throttler.mu.Lock()
	defer func() {
		if !resumed {
			close(ctx.resume)
		}
		if locked {
			throttler.mu.Unlock()
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("admission goroutine did not finish after releasing the fixture")
		}
	}()
	go func() {
		defer close(done)
		_, acquireErr := throttler.TryAcquire(ctx, "inventory")
		result <- acquireErr
	}()
	select {
	case <-ctx.checked:
	case err := <-result:
		t.Fatalf("TryAcquire() returned before cancellation with error %v", err)
	case <-time.After(time.Second):
		t.Fatal("admission did not reach the cancellation checkpoint")
	}
	cancel()
	close(ctx.resume)
	resumed = true
	throttler.mu.Unlock()
	locked = false
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("TryAcquire() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("admission did not finish after cancellation")
	}
	if _, ok := throttler.Snapshot("inventory"); ok {
		t.Fatal("cancellation while waiting for state lock created history")
	}
}

func TestCancellationErrorCallbackDoesNotBlockIndependentStateOperations(t *testing.T) {
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	classifying, release := make(chan struct{}), make(chan struct{})
	ctx := &admissionCheckpointContext{
		Context: base, checked: make(chan struct{}), resume: make(chan struct{}),
		onCanceled: func() {
			close(classifying)
			<-release
		},
	}
	policy, err := NewPolicy(PolicyConfig{
		Revision: "cancellation-callback-v1", MaxRejectionProbability: 0.9,
		MinimumAdmissionProbability: 0.1,
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
	result, done := make(chan error, 1), make(chan struct{})
	stateResult, stateDone := make(chan bool, 1), make(chan struct{})
	locked, resumed, released, stateStarted := true, false, false, false
	throttler.mu.Lock()
	defer func() {
		if !released {
			close(release)
		}
		if !resumed {
			close(ctx.resume)
		}
		if locked {
			throttler.mu.Unlock()
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("admission goroutine did not finish after releasing the fixture")
		}
		if stateStarted {
			select {
			case <-stateDone:
			case <-time.After(time.Second):
				t.Error("state goroutine did not finish after releasing the fixture")
			}
		}
	}()
	go func() {
		defer close(done)
		_, acquireErr := throttler.TryAcquire(ctx, "inventory")
		result <- acquireErr
	}()
	select {
	case <-ctx.checked:
	case err := <-result:
		t.Fatalf("TryAcquire() returned before cancellation with error %v", err)
	case <-time.After(time.Second):
		t.Fatal("admission did not reach the cancellation checkpoint")
	}
	cancel()
	close(ctx.resume)
	resumed = true
	throttler.mu.Unlock()
	locked = false
	select {
	case <-classifying:
	case <-time.After(time.Second):
		t.Fatal("canceled admission did not classify its context error")
	}
	stateStarted = true
	go func() {
		defer close(stateDone)
		recordErr := throttler.Record("independent", Classification{Outcome: Accepted})
		snapshot, ok := throttler.Snapshot("independent")
		stateResult <- recordErr == nil && ok && snapshot.Accepts == 1
	}()
	select {
	case ok := <-stateResult:
		if !ok {
			t.Fatal("independent state operation did not record and snapshot its result")
		}
	case <-time.After(time.Second):
		t.Fatal("context error callback blocked independent state operations")
	}
	close(release)
	released = true
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("TryAcquire() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("admission did not finish after releasing its context callback")
	}
	if _, ok := throttler.Snapshot("inventory"); ok {
		t.Fatal("canceled admission created history")
	}
}
