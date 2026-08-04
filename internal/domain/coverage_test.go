package domain

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// stubDescribable is a minimal Describable used to build error chains without
// importing internal/infra/state (which imports this package's dependents).
type stubDescribable struct {
	reason string
	msg    string
}

func (s *stubDescribable) Error() string               { return s.msg }
func (s *stubDescribable) Reason() string              { return s.reason }
func (s *stubDescribable) Metadata() map[string]string { return map[string]string{"stub": s.reason} }

func TestCoverageContractViolation(t *testing.T) {
	t.Parallel()

	miss := &stubDescribable{reason: ErrReasonCoverageMiss, msg: "coverage miss"}
	plan := &stubDescribable{reason: ErrReasonInvalidExecutionPlan, msg: "bad plan"}
	other := &stubDescribable{reason: ErrReasonNumscriptRuntime, msg: "boom"}

	tests := []struct {
		name string
		err  error
		want Describable
	}{
		{name: "nil", err: nil, want: nil},
		{name: "plain error", err: errors.New("disk on fire"), want: nil},
		{name: "unrelated describable", err: other, want: nil},
		{
			// A genuine stale-inputs error is retryable, not an admission bug —
			// the numscript re-resolution triage depends on the two staying apart.
			name: "stale inputs resolution sentinel",
			err:  ErrStaleInputsResolution,
			want: nil,
		},
		{
			name: "invalid execution plan sentinel type",
			err:  &ErrInvalidExecutionPlan{Reason_: "undeclared key"},
			want: &ErrInvalidExecutionPlan{Reason_: "undeclared key"},
		},
		{name: "bare coverage miss", err: miss, want: miss},
		{name: "bare invalid plan", err: plan, want: plan},
		{
			name: "coverage miss wrapped once",
			err:  &ErrStorageOperation{Operation: "loading ledger", Cause: miss},
			want: miss,
		},
		{
			name: "coverage miss wrapped twice",
			err:  fmt.Errorf("outer: %w", &ErrStorageOperation{Operation: "loading ledger", Cause: miss}),
			want: miss,
		},
		{
			name: "genuine store fault",
			err:  &ErrStorageOperation{Operation: "loading ledger", Cause: errors.New("pebble: closed")},
			want: nil,
		},
		// The multi-error cases below are the branch the forbidigo rule cannot
		// guard: a future errors.Join is a plain call the linter cannot see, so
		// only these rows stand between a regressed descent and a contract
		// violation silently relabelled as a storage fault in the audit chain.
		{
			name: "coverage miss joined with an unrelated error",
			err:  errors.Join(errors.New("pebble: closed"), miss),
			want: miss,
		},
		{
			name: "coverage miss joined behind a wrap",
			err:  fmt.Errorf("commit: %w", errors.Join(miss, errors.New("flush failed"))),
			want: miss,
		},
		{
			name: "multi-%w wrap carrying a coverage miss",
			err:  fmt.Errorf("%w: %w", errors.New("outer"), miss),
			want: miss,
		},
		{
			name: "join with no violation",
			err:  errors.Join(errors.New("pebble: closed"), other),
			want: nil,
		},
		{
			name: "violation nested in a joined subtree",
			err:  errors.Join(errors.New("pebble: closed"), fmt.Errorf("wrap: %w", errors.Join(errors.New("flush failed"), plan))),
			want: plan,
		},
		// This pair pins the slice-order visit CoverageContractViolation
		// documents: the same member set in the opposite order must yield the
		// other violation. A map-based or otherwise unordered traversal would
		// satisfy every row above but not both of these, and a non-deterministic
		// pick on the FSM apply path breaks invariant #2.
		{
			name: "two violations joined, first in slice order wins",
			err:  errors.Join(miss, plan),
			want: miss,
		},
		{
			name: "two violations joined, reversed slice order",
			err:  errors.Join(plan, miss),
			want: plan,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tt.want, CoverageContractViolation(tt.err))
		})
	}
}

func TestStoreFailure(t *testing.T) {
	t.Parallel()

	t.Run("propagates a coverage miss verbatim", func(t *testing.T) {
		t.Parallel()

		miss := &stubDescribable{reason: ErrReasonCoverageMiss, msg: "coverage miss"}

		got := StoreFailure("loading ledger", miss)

		require.Same(t, miss, got)
		require.Equal(t, ErrReasonCoverageMiss, got.Reason())
		require.Equal(t, map[string]string{"stub": ErrReasonCoverageMiss}, got.Metadata())
	})

	t.Run("propagates a wrapped coverage miss verbatim", func(t *testing.T) {
		t.Parallel()

		miss := &stubDescribable{reason: ErrReasonCoverageMiss, msg: "coverage miss"}

		got := StoreFailure("loading ledger", fmt.Errorf("numscript: %w", miss))

		require.Same(t, miss, got)
	})

	t.Run("propagates a joined coverage miss verbatim", func(t *testing.T) {
		t.Parallel()

		miss := &stubDescribable{reason: ErrReasonCoverageMiss, msg: "coverage miss"}

		got := StoreFailure("loading ledger", errors.Join(errors.New("pebble: closed"), miss))

		require.Same(t, miss, got)
	})

	t.Run("wraps a genuine store fault", func(t *testing.T) {
		t.Parallel()

		cause := errors.New("pebble: closed")

		got := StoreFailure("loading ledger", cause)

		require.Equal(t, ErrReasonStorageOperation, got.Reason())
		require.Equal(t, map[string]string{"operation": "loading ledger"}, got.Metadata())
		require.ErrorIs(t, got, cause)
	})
}
