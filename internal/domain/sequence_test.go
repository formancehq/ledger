package domain

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCheckedNextSequence(t *testing.T) {
	t.Parallel()

	t.Run("ordinary value", func(t *testing.T) {
		t.Parallel()

		next, err := CheckedNextSequence(41, SequenceCounterTransactionID)
		require.Nil(t, err)
		require.Equal(t, uint64(42), next)
	})

	t.Run("max minus one remains valid", func(t *testing.T) {
		t.Parallel()

		next, err := CheckedNextSequence(math.MaxUint64-1, SequenceCounterLog)
		require.Nil(t, err)
		require.Equal(t, uint64(math.MaxUint64), next)
	})

	t.Run("max fails explicitly instead of wrapping", func(t *testing.T) {
		t.Parallel()

		next, err := CheckedNextSequence(math.MaxUint64, SequenceCounterAudit)
		require.Zero(t, next)
		require.Equal(t, SequenceCounterAudit, err.Counter)
		require.Equal(t, ErrReasonSequenceExhausted, err.Reason())
		require.Equal(t, map[string]string{"counter": "auditSequence"}, err.Metadata())
	})
}
