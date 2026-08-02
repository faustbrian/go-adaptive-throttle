package throttle_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

func ExampleExecute() {
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "catalog-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 120},
		MinimumSamples:              20,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                100,
	})
	if err != nil {
		panic(err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		panic(err)
	}
	value, err := throttle.Execute(context.Background(), throttler, "catalog-api", func(context.Context) (string, error) {
		return "available", nil
	})
	fmt.Println(value, err)
	// Output: available <nil>
}

func ExampleThrottler_TryAcquire() {
	policy, err := throttle.NewPolicy(throttle.PolicyConfig{
		Revision:                    "manual-v1",
		Window:                      throttle.WindowConfig{BucketDuration: time.Second, BucketCount: 60},
		MinimumSamples:              10,
		Algorithm:                   throttle.GoogleSRE{AcceptMultiplier: 2},
		MaxRejectionProbability:     0.9,
		MinimumAdmissionProbability: 0.1,
		MaxResources:                10,
	})
	if err != nil {
		panic(err)
	}
	throttler, err := throttle.New(policy)
	if err != nil {
		panic(err)
	}
	permit, err := throttler.TryAcquire(context.Background(), "payments")
	if errors.Is(err, throttle.ErrRejected) {
		return
	}
	if err != nil {
		panic(err)
	}
	_ = permit.Record(throttle.Classification{Outcome: throttle.Accepted, Reason: throttle.ReasonSuccess})
	fmt.Println("recorded")
	// Output: recorded
}
