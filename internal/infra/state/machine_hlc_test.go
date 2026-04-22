package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
)

func TestHLCTimestamp(t *testing.T) {
	t.Parallel()

	t.Run("proposal date ahead of last applied uses proposal date", func(t *testing.T) {
		t.Parallel()

		machine, _, _ := newTestMachine(t)
		machine.lastAppliedTimestamp = 1000

		result := machine.hlcTimestamp(&commonpb.Timestamp{Data: 2000})

		require.Equal(t, uint64(2000), result.GetData())
		require.Equal(t, uint64(2000), machine.lastAppliedTimestamp)
	})

	t.Run("proposal date behind last applied increments by 1", func(t *testing.T) {
		t.Parallel()

		machine, _, _ := newTestMachine(t)
		machine.lastAppliedTimestamp = 5000

		result := machine.hlcTimestamp(&commonpb.Timestamp{Data: 3000})

		require.Equal(t, uint64(5001), result.GetData())
		require.Equal(t, uint64(5001), machine.lastAppliedTimestamp)
	})

	t.Run("proposal date equal to last applied increments by 1", func(t *testing.T) {
		t.Parallel()

		machine, _, _ := newTestMachine(t)
		machine.lastAppliedTimestamp = 5000

		result := machine.hlcTimestamp(&commonpb.Timestamp{Data: 5000})

		require.Equal(t, uint64(5001), result.GetData())
		require.Equal(t, uint64(5001), machine.lastAppliedTimestamp)
	})

	t.Run("monotonicity across multiple proposals", func(t *testing.T) {
		t.Parallel()

		machine, _, _ := newTestMachine(t)
		machine.lastAppliedTimestamp = 0

		// Simulate a sequence of proposals with varying dates
		proposalDates := []uint64{100, 200, 150, 150, 300, 250, 250, 250}

		var timestamps []uint64

		for _, date := range proposalDates {
			result := machine.hlcTimestamp(&commonpb.Timestamp{Data: date})
			timestamps = append(timestamps, result.GetData())
		}

		// Verify strict monotonicity: each timestamp > previous
		for i := 1; i < len(timestamps); i++ {
			require.Greater(t, timestamps[i], timestamps[i-1],
				"timestamp[%d]=%d should be > timestamp[%d]=%d",
				i, timestamps[i], i-1, timestamps[i-1])
		}

		// Verify expected values:
		// 100 (ahead), 200 (ahead), 201 (behind 200), 202 (behind 201),
		// 300 (ahead), 301 (behind 300), 302 (behind 301), 303 (behind 302)
		expected := []uint64{100, 200, 201, 202, 300, 301, 302, 303}
		require.Equal(t, expected, timestamps)
	})
}

func TestHLCTimestampIntegration(t *testing.T) {
	t.Parallel()

	t.Run("HLC advances and persists on clock regression", func(t *testing.T) {
		t.Parallel()

		machine, dataStore, _ := newTestMachine(t)
		ctx := context.Background()

		const ledgerName = "hlc-test"

		// Create ledger with a high timestamp
		result, err := machine.ApplyEntries(ctx,
			makeEntry(t, 1, &raftcmdpb.Proposal{
				Id:     1,
				Orders: []*raftcmdpb.Order{createLedgerOrder(ledgerName)},
				Date:   &commonpb.Timestamp{Data: 1000000},
			}),
		)
		require.NoError(t, err)
		require.Len(t, result.Results, 1)
		require.NoError(t, result.Results[0].Error)
		require.Equal(t, uint64(1000000), machine.lastAppliedTimestamp)

		// Create a transaction with a lower timestamp (clock regression)
		txOrders := []*raftcmdpb.Order{
			createTransactionOrder(ledgerName, true,
				newPosting("world", "user:alice", "EUR", 100),
			),
		}
		result, err = machine.ApplyEntries(ctx,
			makeEntry(t, 2, &raftcmdpb.Proposal{
				Id:      2,
				Orders:  txOrders,
				Date:    &commonpb.Timestamp{Data: 500000}, // Behind last applied
				Preload: &raftcmdpb.PreloadSet{Preloads: buildVolumePreloads(txOrders)},
			}),
		)
		require.NoError(t, err)
		require.Len(t, result.Results, 1)
		require.NoError(t, result.Results[0].Error)

		// The effective timestamp should be 1000001 (last + 1), not 500000
		require.Equal(t, uint64(1000001), machine.lastAppliedTimestamp,
			"HLC should advance past last timestamp on clock regression")

		// Verify the timestamp was persisted in the store
		persistedTS, err := query.ReadLastAppliedTimestamp(dataStore)
		require.NoError(t, err)
		require.Equal(t, uint64(1000001), persistedTS)
	})

	t.Run("HLC advances when proposal date is ahead", func(t *testing.T) {
		t.Parallel()

		machine, dataStore, _ := newTestMachine(t)
		ctx := context.Background()

		const ledgerName = "hlc-ahead-test"

		// Create ledger with timestamp 1000
		result, err := machine.ApplyEntries(ctx,
			makeEntry(t, 1, &raftcmdpb.Proposal{
				Id:     1,
				Orders: []*raftcmdpb.Order{createLedgerOrder(ledgerName)},
				Date:   &commonpb.Timestamp{Data: 1000},
			}),
		)
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)

		// Create transaction with a higher timestamp
		txOrders := []*raftcmdpb.Order{
			createTransactionOrder(ledgerName, true,
				newPosting("world", "user:alice", "EUR", 100),
			),
		}
		result, err = machine.ApplyEntries(ctx,
			makeEntry(t, 2, &raftcmdpb.Proposal{
				Id:      2,
				Orders:  txOrders,
				Date:    &commonpb.Timestamp{Data: 5000},
				Preload: &raftcmdpb.PreloadSet{Preloads: buildVolumePreloads(txOrders)},
			}),
		)
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)

		require.Equal(t, uint64(5000), machine.lastAppliedTimestamp,
			"HLC should use proposal date when it is ahead")

		persistedTS, err := query.ReadLastAppliedTimestamp(dataStore)
		require.NoError(t, err)
		require.Equal(t, uint64(5000), persistedTS)
	})

	t.Run("HLC timestamp persisted in Pebble", func(t *testing.T) {
		t.Parallel()

		machine, dataStore, _ := newTestMachine(t)
		ctx := context.Background()

		const ledgerName = "hlc-snapshot-test"

		// Apply an entry to advance the HLC
		result, err := machine.ApplyEntries(ctx,
			makeEntry(t, 1, &raftcmdpb.Proposal{
				Id:     1,
				Orders: []*raftcmdpb.Order{createLedgerOrder(ledgerName)},
				Date:   &commonpb.Timestamp{Data: 9999999},
			}),
		)
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)

		// Verify timestamp is persisted in Pebble.
		lastTimestamp, err := query.ReadLastAppliedTimestamp(dataStore)
		require.NoError(t, err)
		require.Equal(t, uint64(9999999), lastTimestamp)
	})

	t.Run("monotonicity across multiple entries", func(t *testing.T) {
		t.Parallel()

		machine, _, _ := newTestMachine(t)
		ctx := context.Background()

		const ledgerName = "hlc-mono-test"

		// Create ledger
		result, err := machine.ApplyEntries(ctx,
			makeEntry(t, 1, &raftcmdpb.Proposal{
				Id:     1,
				Orders: []*raftcmdpb.Order{createLedgerOrder(ledgerName)},
				Date:   &commonpb.Timestamp{Data: 1000},
			}),
		)
		require.NoError(t, err)
		require.NoError(t, result.Results[0].Error)

		// Apply entries with regressing timestamps
		timestamps := make([]uint64, 0, 5)
		timestamps = append(timestamps, machine.lastAppliedTimestamp)

		proposalDates := []uint64{900, 800, 700, 2000, 1500}
		for i, date := range proposalDates {
			txOrders := []*raftcmdpb.Order{
				createTransactionOrder(ledgerName, true,
					newPosting("world", "user:alice", "EUR", 10),
				),
			}
			result, err := machine.ApplyEntries(ctx,
				makeEntry(t, uint64(i+2), &raftcmdpb.Proposal{
					Id:      uint64(i + 2),
					Orders:  txOrders,
					Date:    &commonpb.Timestamp{Data: date},
					Preload: &raftcmdpb.PreloadSet{Preloads: buildVolumePreloads(txOrders)},
				}),
			)
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)

			timestamps = append(timestamps, machine.lastAppliedTimestamp)
		}

		// Verify strict monotonicity
		for i := 1; i < len(timestamps); i++ {
			require.Greater(t, timestamps[i], timestamps[i-1],
				"timestamp[%d]=%d should be > timestamp[%d]=%d",
				i, timestamps[i], i-1, timestamps[i-1])
		}
	})
}
