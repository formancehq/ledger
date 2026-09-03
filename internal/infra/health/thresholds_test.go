package health

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestThresholdsAnyAtBlock(t *testing.T) {
	t.Parallel()
	th := Thresholds{WALBlock: 0.8, WALResume: 0.75, DataBlock: 0.8, DataResume: 0.75}

	require.True(t, th.anyAtBlock([]VolumeSample{{WALFraction: 0.81, WALValid: true, DataFraction: 0.1, DataValid: true}}))
	require.False(t, th.anyAtBlock([]VolumeSample{{WALFraction: 0.5, WALValid: true, DataFraction: 0.5, DataValid: true}}))
	// Peer over data block trips even if local is fine.
	require.True(t, th.anyAtBlock([]VolumeSample{
		{WALFraction: 0.1, WALValid: true, DataFraction: 0.1, DataValid: true},
		{WALFraction: 0.1, WALValid: true, DataFraction: 0.9, DataValid: true},
	}))
	// Exactly at the block mark trips (>=).
	require.True(t, th.anyAtBlock([]VolumeSample{{WALValid: true, DataFraction: 0.8, DataValid: true}}))
	// An invalid high value is diagnostic only and cannot create a new block.
	require.False(t, th.anyAtBlock([]VolumeSample{{WALFraction: 0.99, DataFraction: 0.99}}))
}

func TestThresholdsAllBelowResume(t *testing.T) {
	t.Parallel()
	th := Thresholds{WALBlock: 0.8, WALResume: 0.75, DataBlock: 0.8, DataResume: 0.75}

	require.True(t, th.allBelowResume([]VolumeSample{{WALFraction: 0.7, WALValid: true, DataFraction: 0.7, DataValid: true}}))
	// One volume still in the band -> not cleared.
	require.False(t, th.allBelowResume([]VolumeSample{{WALFraction: 0.76, WALValid: true, DataFraction: 0.1, DataValid: true}}))
	// Exactly at resume mark is NOT below (>=) -> not cleared.
	require.False(t, th.allBelowResume([]VolumeSample{{WALValid: true, DataFraction: 0.75, DataValid: true}}))
	// Missing freshness prevents an existing block from being cleared.
	require.False(t, th.allBelowResume([]VolumeSample{{WALValid: true, DataValid: false}}))
}

func TestNextDiskBlockedHysteresis(t *testing.T) {
	t.Parallel()
	th := Thresholds{WALBlock: 0.8, WALResume: 0.75, DataBlock: 0.8, DataResume: 0.75}

	// not blocked + below block -> stays unblocked
	require.False(t, th.NextDiskBlocked(false, []VolumeSample{{WALValid: true, DataFraction: 0.5, DataValid: true}}))
	// not blocked + at block -> blocks
	require.True(t, th.NextDiskBlocked(false, []VolumeSample{{WALValid: true, DataFraction: 0.8, DataValid: true}}))
	// blocked + in band (0.75..0.8) -> stays blocked (hysteresis)
	require.True(t, th.NextDiskBlocked(true, []VolumeSample{{WALValid: true, DataFraction: 0.77, DataValid: true}}))
	// blocked + below resume -> clears
	require.False(t, th.NextDiskBlocked(true, []VolumeSample{{WALValid: true, DataFraction: 0.74, DataValid: true}}))
	// Invalid measurements neither create a new block nor clear an existing one.
	require.False(t, th.NextDiskBlocked(false, []VolumeSample{{}}))
	require.True(t, th.NextDiskBlocked(true, []VolumeSample{{}}))
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
