package throttle_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

func TestExecuteClassifiesOneAdmittedExecution(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	var classifications atomic.Int64
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "execute-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 10},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0.99},
		Classifier: func(completion throttle.Completion) throttle.Classification {
			classifications.Add(1)
			if !errors.Is(completion.Err, errOverloaded) {
				t.Fatalf("classifier error = %v, want errOverloaded", completion.Err)
			}
			return throttle.Classification{Outcome: throttle.DownstreamOverload, Reason: throttle.ReasonExplicitOverload}
		},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}

	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	invocations := 0
	_, err = throttle.Execute(context.Background(), throttler, "inventory", func(context.Context) (string, error) {
		invocations++
		return "", errOverloaded
	})
	if !errors.Is(err, errOverloaded) {
		t.Fatalf("Execute() error = %v, want errOverloaded", err)
	}
	if invocations != 1 || classifications.Load() != 1 {
		t.Fatalf("invocations = %d, classifications = %d, want 1 and 1", invocations, classifications.Load())
	}
	snapshot, _ := throttler.Snapshot("inventory")
	if snapshot.Requests != 1 || snapshot.Samples != 1 || snapshot.Overloads != 1 || snapshot.Accepts != 0 {
		t.Fatalf("Snapshot() = %+v, want one overload sample", snapshot)
	}
}

var errOverloaded = errors.New("downstream overloaded")

func TestDefaultClassifierExcludesLocalDeadlineFromOverloadHistory(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "classification-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 10},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
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
	_, err = throttle.Execute(context.Background(), throttler, "inventory", func(context.Context) (struct{}, error) {
		return struct{}{}, context.DeadlineExceeded
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute() error = %v, want context deadline", err)
	}
	snapshot, _ := throttler.Snapshot("inventory")
	if snapshot.Requests != 0 || snapshot.Samples != 0 || snapshot.Overloads != 0 || snapshot.Ignored != 1 {
		t.Fatalf("Snapshot() = %+v, want deadline excluded with one ignored observation", snapshot)
	}
}

func TestDefaultClassifierExcludesUnknownPolicyRejectionFromDownstreamHistory(t *testing.T) {
	t.Parallel()

	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "classification-safety-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 2},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       &fixedClock{now: time.Unix(1_700_000_000, 0)},
		Random:                      fixedRandom{value: 0.99},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	localPolicyRejection := errors.New("other policy rejected")
	_, operationErr := throttle.Execute(context.Background(), throttler, "backend", func(context.Context) (struct{}, error) {
		return struct{}{}, localPolicyRejection
	})
	if !errors.Is(operationErr, localPolicyRejection) {
		t.Fatalf("Execute() error = %v, want local policy rejection", operationErr)
	}
	snapshot, _ := throttler.Snapshot("backend")
	if snapshot.Ignored != 1 || snapshot.Requests != 0 || snapshot.Accepts != 0 || snapshot.Samples != 0 || snapshot.Failures != 0 || snapshot.Overloads != 0 {
		t.Fatalf("Snapshot() = %+v, unknown policy rejection must not become a downstream sample", snapshot)
	}
}

func TestExecuteDoesNotInvokeRejectedOperation(t *testing.T) {
	t.Parallel()

	throttler := newTestThrottler(t, fixedRandom{value: 0})
	if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	invoked := false
	_, err := throttle.Execute(context.Background(), throttler, "inventory", func(context.Context) (struct{}, error) {
		invoked = true
		return struct{}{}, nil
	})
	if !errors.Is(err, throttle.ErrRejected) || invoked {
		t.Fatalf("Execute() error = %v, invoked = %t, want rejection without invocation", err, invoked)
	}
}
