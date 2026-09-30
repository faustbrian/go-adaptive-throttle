//go:build !race

package throttle_test

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	throttle "github.com/faustbrian/go-adaptive-throttle"
)

func TestRetainedResourceKeysDoNotKeepOversizedCallerBuffersAlive(t *testing.T) {
	const resources = 32

	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "resource-ownership-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 1},
		MinimumSamples:              1,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 1},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                resources,
		Clock:                       &fixedClock{now: time.Unix(1_700_000_000, 0)},
		Random:                      fixedRandom{value: 0.99},
	})
	if err != nil {
		t.Fatal(err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for index := range resources {
		key := fmt.Sprintf("resource-%02d", index)
		callerBuffer := key + strings.Repeat("x", 1<<20)
		permit, acquireErr := throttler.TryAcquire(context.Background(), callerBuffer[:len(key)])
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		if recordErr := permit.Record(throttle.Classification{Outcome: throttle.Accepted}); recordErr != nil {
			t.Fatal(recordErr)
		}
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if retained := int64(after.HeapAlloc) - int64(before.HeapAlloc); retained > 8<<20 {
		t.Fatalf("retained resource buffers use %d bytes, want at most %d", retained, 8<<20)
	}
	runtime.KeepAlive(throttler)
}

func TestPolicyRevisionDoesNotKeepOversizedCallerBufferAlive(t *testing.T) {
	const policiesCount = 32

	policies := make([]throttle.Policy, 0, policiesCount)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for index := range policiesCount {
		revision := fmt.Sprintf("revision-%02d", index)
		callerBuffer := revision + strings.Repeat("x", 1<<20)
		policy, err := throttle.NewPolicy(throttle.PolicyConfig{
			Revision:                    callerBuffer[:len(revision)],
			MaxRejectionProbability:     0.9,
			MinimumAdmissionProbability: 0.1,
		})
		if err != nil {
			t.Fatal(err)
		}
		policies = append(policies, policy)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if retained := int64(after.HeapAlloc) - int64(before.HeapAlloc); retained > 8<<20 {
		t.Fatalf("retained revision buffers use %d bytes, want at most %d", retained, 8<<20)
	}
	runtime.KeepAlive(policies)
}

func TestOutstandingPermitsReuseOwnedResourceKey(t *testing.T) {
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "permit-ownership-v1",
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                1,
	})
	if err != nil {
		t.Fatal(err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		t.Fatal(err)
	}
	if err := throttler.Record("inventory", throttle.Classification{Outcome: throttle.Accepted}); err != nil {
		t.Fatal(err)
	}
	permits := make([]*throttle.Permit, 0, 32)
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 32 {
		callerBuffer := "inventory" + strings.Repeat("x", 1<<20)
		permit, acquireErr := throttler.TryAcquire(context.Background(), callerBuffer[:len("inventory")])
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		permits = append(permits, permit)
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if retained := int64(after.HeapAlloc) - int64(before.HeapAlloc); retained > 8<<20 {
		t.Fatalf("outstanding permits retain %d bytes, want at most %d", retained, 8<<20)
	}
	runtime.KeepAlive(permits)
}
