package health

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestThresholdsAnyAtBlock(t *testing.T) {
	t.Parallel()
	th := Thresholds{WALBlock: 0.8, WALResume: 0.75, DataBlock: 0.8, DataResume: 0.75}

	require.True(t, th.anyAtBlock([]VolumeSample{{WALFraction: 0.81, DataFraction: 0.1}}))
	require.False(t, th.anyAtBlock([]VolumeSample{{WALFraction: 0.5, DataFraction: 0.5}}))
	// Peer over data block trips even if local is fine.
	require.True(t, th.anyAtBlock([]VolumeSample{
		{WALFraction: 0.1, DataFraction: 0.1},
		{WALFraction: 0.1, DataFraction: 0.9},
	}))
	// Exactly at the block mark trips (>=).
	require.True(t, th.anyAtBlock([]VolumeSample{{DataFraction: 0.8}}))
}

func TestThresholdsAllBelowResume(t *testing.T) {
	t.Parallel()
	th := Thresholds{WALBlock: 0.8, WALResume: 0.75, DataBlock: 0.8, DataResume: 0.75}

	require.True(t, th.allBelowResume([]VolumeSample{{WALFraction: 0.7, DataFraction: 0.7}}))
	// One volume still in the band -> not cleared.
	require.False(t, th.allBelowResume([]VolumeSample{{WALFraction: 0.76, DataFraction: 0.1}}))
	// Exactly at resume mark is NOT below (>=) -> not cleared.
	require.False(t, th.allBelowResume([]VolumeSample{{DataFraction: 0.75}}))
}

func TestNextDiskBlockedHysteresis(t *testing.T) {
	t.Parallel()
	th := Thresholds{WALBlock: 0.8, WALResume: 0.75, DataBlock: 0.8, DataResume: 0.75}

	// not blocked + below block -> stays unblocked
	require.False(t, th.NextDiskBlocked(false, []VolumeSample{{DataFraction: 0.5}}))
	// not blocked + at block -> blocks
	require.True(t, th.NextDiskBlocked(false, []VolumeSample{{DataFraction: 0.8}}))
	// blocked + in band (0.75..0.8) -> stays blocked (hysteresis)
	require.True(t, th.NextDiskBlocked(true, []VolumeSample{{DataFraction: 0.77}}))
	// blocked + below resume -> clears
	require.False(t, th.NextDiskBlocked(true, []VolumeSample{{DataFraction: 0.74}}))
	// empty samples (no nodes measured) -> not blocked when previously unblocked
	require.False(t, th.NextDiskBlocked(false, nil))
	// no samples while blocked -> hold blocked
	require.True(t, th.NextDiskBlocked(true, nil))
}

func TestWriteGateErrorForState(t *testing.T) {
	t.Parallel()
	require.NoError(t, writeGateErrorForState(false, false))
	require.ErrorIs(t, writeGateErrorForState(true, false), domain.ErrWritesBlockedDiskFull)
	require.ErrorIs(t, writeGateErrorForState(false, true), domain.ErrWritesBlockedClockSkew)
	// disk takes precedence when both are set.
	require.ErrorIs(t, writeGateErrorForState(true, true), domain.ErrWritesBlockedDiskFull)
}
