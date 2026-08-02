package throttle_test

import (
	"testing"

	throttle "github.com/faustbrian/golib/pkg/adaptive-throttle"
)

func TestNewRejectsZeroPolicy(t *testing.T) {
	t.Parallel()

	throttler, err := throttle.New(throttle.Policy{})
	if err == nil || throttler != nil {
		t.Fatalf("New(zero policy) = (%v, %v), want nil and error", throttler, err)
	}
}
