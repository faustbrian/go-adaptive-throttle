package throttle_test

import (
	"context"
	"errors"
	"testing"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

func TestHealthyOutageRecoveryAndColdReplicaSimulation(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "simulation-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 120},
		MinimumSamples:              10,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0.5},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	loaded, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New(loaded) error = %v", err)
	}
	cold, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New(cold) error = %v", err)
	}
	for range 100 {
		if err := loaded.Record("backend", throttle.Classification{Outcome: throttle.Accepted}); err != nil {
			t.Fatalf("healthy Record() error = %v", err)
		}
	}
	if snapshot, _ := loaded.Snapshot("backend"); snapshot.RejectionProbability != 0 {
		t.Fatalf("healthy probability = %v, want zero", snapshot.RejectionProbability)
	}
	for range 150 {
		if err := loaded.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
			t.Fatalf("outage Record() error = %v", err)
		}
	}
	loadedSnapshot, _ := loaded.Snapshot("backend")
	if loadedSnapshot.RejectionProbability <= 0 {
		t.Fatalf("outage probability = %v, want positive", loadedSnapshot.RejectionProbability)
	}
	if _, ok := cold.Snapshot("backend"); ok {
		t.Fatal("cold replica inherited another process's history")
	}
	for range 300 {
		if err := loaded.Record("backend", throttle.Classification{Outcome: throttle.Accepted}); err != nil {
			t.Fatalf("recovery Record() error = %v", err)
		}
	}
	recovered, _ := loaded.Snapshot("backend")
	if recovered.RejectionProbability != 0 {
		t.Fatalf("recovered probability = %v, want zero", recovered.RejectionProbability)
	}
}

func TestSparsePartialRampBurstAndOscillationSimulation(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "traffic-shapes-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 4},
		MinimumSamples:              5,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
		Clock:                       clock,
		Random:                      fixedRandom{value: 0.5},
	})
	if err != nil {
		t.Fatalf("NewPolicy() error = %v", err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	for range 4 {
		if err := throttler.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
			t.Fatalf("sparse Record() error = %v", err)
		}
	}
	if snapshot, _ := throttler.Snapshot("backend"); snapshot.RejectionProbability != 0 {
		t.Fatalf("sparse probability = %v, want zero before minimum samples", snapshot.RejectionProbability)
	}
	if err := throttler.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload}); err != nil {
		t.Fatalf("minimum-sample Record() error = %v", err)
	}
	if snapshot, _ := throttler.Snapshot("backend"); snapshot.RejectionProbability <= 0 {
		t.Fatalf("minimum-sample probability = %v, want positive", snapshot.RejectionProbability)
	}

	throttler.ResetAll()
	for range 2 {
		_ = throttler.Record("backend", throttle.Classification{Outcome: throttle.Accepted})
	}
	for range 3 {
		_ = throttler.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload})
	}
	partial, _ := throttler.Snapshot("backend")
	for range 5 {
		_ = throttler.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload})
	}
	ramp, _ := throttler.Snapshot("backend")
	if partial.RejectionProbability <= 0 || ramp.RejectionProbability <= partial.RejectionProbability {
		t.Fatalf("ramp probabilities = (%v, %v), want increasing positive shedding", partial.RejectionProbability, ramp.RejectionProbability)
	}

	clock.now = clock.now.Add(4 * time.Second)
	if expired, _ := throttler.Snapshot("backend"); expired.Samples != 0 || expired.RejectionProbability != 0 {
		t.Fatalf("expired burst Snapshot() = %+v", expired)
	}
	for range 10 {
		_ = throttler.Record("backend", throttle.Classification{Outcome: throttle.Accepted})
		_ = throttler.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload})
	}
	if oscillating, _ := throttler.Snapshot("backend"); oscillating.RejectionProbability != 0 {
		t.Fatalf("oscillating probability = %v, want zero", oscillating.RejectionProbability)
	}
}

func TestReplicaRevisionSIGTERMDrainAbruptDeathAndCorrelatedDecisionSimulation(t *testing.T) {
	t.Parallel()

	clock := &fixedClock{now: time.Unix(1_700_000_000, 0)}
	newReplica := func(revision string, sample float64) *throttle.Throttler {
		t.Helper()
		policy, err := throttle.NewPolicy(throttle.PolicyConfig{
			Revision:                    revision,
			Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 10},
			MinimumSamples:              1,
			Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
			MaxRejectionProbability:     0.9,
			MinimumAdmissionProbability: 0.1,
			MaxResources:                1,
			Clock:                       clock,
			Random:                      fixedRandom{value: sample},
		})
		if err != nil {
			t.Fatalf("NewPolicy() error = %v", err)
		}
		replica, err := throttle.New(policy)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		return replica
	}

	loaded := newReplica("policy-v1", 0.2)
	peer := newReplica("policy-v2", 0.9)
	for range 10 {
		_ = loaded.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload})
		_ = peer.Record("backend", throttle.Classification{Outcome: throttle.DownstreamOverload})
	}
	loadedSnapshot, _ := loaded.Snapshot("backend")
	peerSnapshot, _ := peer.Snapshot("backend")
	if loadedSnapshot.Revision == peerSnapshot.Revision || loadedSnapshot.RejectionProbability != peerSnapshot.RejectionProbability {
		t.Fatalf("mixed revision snapshots = (%+v, %+v)", loadedSnapshot, peerSnapshot)
	}
	if _, err := loaded.TryAcquire(context.Background(), "backend"); !errors.Is(err, throttle.ErrRejected) {
		t.Fatalf("loaded TryAcquire() error = %v, want ErrRejected", err)
	}
	permit, err := peer.TryAcquire(context.Background(), "backend")
	if err != nil {
		t.Fatalf("peer TryAcquire() error = %v", err)
	}
	if removed := peer.ResetAll(); removed != 1 {
		t.Fatalf("drain ResetAll() = %d, want 1", removed)
	}
	if err := permit.Record(throttle.Classification{Outcome: throttle.Accepted}); err != nil {
		t.Fatalf("drained Permit.Record() error = %v", err)
	}
	if _, ok := peer.Snapshot("backend"); ok {
		t.Fatal("drained permit recreated discarded replica history")
	}
	if cold := newReplica("policy-v2", 0.9); len(cold.Snapshots()) != 0 {
		t.Fatal("replacement replica inherited scale-down history")
	}
}
