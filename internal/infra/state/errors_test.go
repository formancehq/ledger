package state

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestErrNodeOutOfSync(t *testing.T) {
	t.Parallel()

	err := &ErrNodeOutOfSync{
		SnapshotIndex:    100,
		LastAppliedIndex: 50,
	}
	msg := err.Error()
	require.Contains(t, msg, "100")
	require.Contains(t, msg, "50")
	require.Contains(t, msg, "out of sync")
}

func TestErrInvalidEntryIndex(t *testing.T) {
	t.Parallel()

	err := &ErrInvalidEntryIndex{
		ReceivedIndex: 7,
		ExpectedIndex: 5,
	}
	msg := err.Error()
	require.Contains(t, msg, "7")
	require.Contains(t, msg, "5")
	require.Contains(t, msg, "invalid index")
}

func TestErrDoubleEntryInvariantViolated(t *testing.T) {
	t.Parallel()

	err := &ErrDoubleEntryInvariantViolated{
		InputSum:  "1000",
		OutputSum: "900",
	}
	msg := err.Error()
	require.Contains(t, msg, "1000")
	require.Contains(t, msg, "900")
	require.Contains(t, msg, "double-entry invariant violated")
}
