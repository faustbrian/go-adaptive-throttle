package throttle

import (
	"context"
	"errors"
	"testing"
)

func TestDefaultClassifierIsConservative(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err     error
		outcome Outcome
		reason  Reason
	}{
		{err: nil, outcome: Accepted, reason: ReasonSuccess},
		{err: context.Canceled, outcome: Ignored, reason: ReasonCallerCanceled},
		{err: context.DeadlineExceeded, outcome: Ignored, reason: ReasonCallerDeadline},
		{err: ErrRejected, outcome: Ignored, reason: ReasonLocalPolicy},
		{err: errors.New("ordinary failure"), outcome: DownstreamFailure, reason: ReasonDownstreamFailure},
	}
	for _, test := range tests {
		classification := defaultClassifier(Completion{Err: test.err})
		if classification.Outcome != test.outcome || classification.Reason != test.reason {
			t.Fatalf("defaultClassifier(%v) = %+v, want outcome %v reason %v", test.err, classification, test.outcome, test.reason)
		}
	}
}

func TestClassifierAndPriorityPanicsFailSafely(t *testing.T) {
	t.Parallel()

	classification := safeClassify(func(Completion) Classification { panic("classifier") }, Completion{})
	if classification.Outcome != Ignored {
		t.Fatalf("safeClassify(panic) = %+v, want ignored", classification)
	}
	classification = safeClassify(func(Completion) Classification { return Classification{Outcome: LocalRejection} }, Completion{})
	if classification.Outcome != Ignored {
		t.Fatalf("safeClassify(local rejection) = %+v, want ignored", classification)
	}
	priority := safePriority(func(context.Context) Priority { panic("priority") }, context.Background(), 2)
	if priority != 0 {
		t.Fatalf("safePriority(panic) = %d, want zero", priority)
	}
}
