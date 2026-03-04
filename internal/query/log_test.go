package query_test

import (
	"context"
	"math/big"
	"testing"

	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/stretchr/testify/require"
)

func TestReadLogBySequence(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	registerLedger(t, s, "test-ledger")
	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	log, err := query.ReadLogBySequence(context.Background(), s,1)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(1), log.Sequence)

	log, err = query.ReadLogBySequence(context.Background(), s,999)
	require.NoError(t, err)
	require.Nil(t, log)
}

func TestReadLastSequence(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	registerLedger(t, s, "test-ledger")

	// Test with no logs - should return 0
	lastSequence, err := query.ReadLastSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastSequence)

	// Insert logs and verify last sequence
	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	lastSequence, err = query.ReadLastSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastSequence) // Last log has sequence 4
}

func TestReadLastSequenceAfterSnapshot(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Create some data
	registerLedger(t, s, "test-ledger")
	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 0, testLogs...)

	// Create snapshot
	checkpointID, err := s.CreateSnapshot()
	require.NoError(t, err)
	require.Equal(t, uint64(1), checkpointID)

	// Verify data still accessible after snapshot
	lastSequence, err := query.ReadLastSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastSequence)
}

func TestReadLogsSince(t *testing.T) {
	t.Parallel()

	t.Run("EmptyStore", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		cursor, err := query.ReadLogsSince(context.Background(), s,0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("AllLogs", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// afterSequence=0 should return all logs
		cursor, err := query.ReadLogsSince(context.Background(), s,0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 4)
		require.Equal(t, uint64(1), logs[0].Sequence)
		require.Equal(t, uint64(4), logs[3].Sequence)
	})

	t.Run("LogsAfterSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// afterSequence=2 should return logs 3 and 4
		cursor, err := query.ReadLogsSince(context.Background(), s,2)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 2)
		require.Equal(t, uint64(3), logs[0].Sequence)
		require.Equal(t, uint64(4), logs[1].Sequence)
	})

	t.Run("LogsAfterLastSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// afterSequence=4 (last log) should return empty
		cursor, err := query.ReadLogsSince(context.Background(), s,4)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("LogsAfterFarFutureSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		cursor, err := query.ReadLogsSince(context.Background(), s,999)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("IncrementalRead", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger")
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// Simulate emitter: read all, then read after cursor
		cursor, err := query.ReadLogsSince(context.Background(), s,0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 4)

		lastSeq := logs[len(logs)-1].Sequence

		// Append more logs
		moreLogs := createTestLogsForLedger("test-ledger", 5)
		appendLogs(t, s, 2, moreLogs...)

		// Read only new logs
		cursor, err = query.ReadLogsSince(context.Background(), s,lastSeq)
		require.NoError(t, err)
		newLogs := collectLogs(t, cursor)
		require.Len(t, newLogs, 4) // 4 new logs starting from sequence 5
		require.Equal(t, uint64(5), newLogs[0].Sequence)
	})

	t.Run("LogPayloadTypes", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		now := libtime.Now()
		registerLedger(t, s, "test-ledger")

		// Create logs with different payload types
		mixedLogs := []*commonpb.Log{
			{
				Sequence: 1,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: &commonpb.CreateLedgerLog{
							Info: &commonpb.LedgerInfo{
								Name:      "new-ledger",
								CreatedAt: commonpb.NewTimestamp(now),
							},
						},
					},
				},
			},
			{
				Sequence: 2,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "test-ledger",
							Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
									CreatedTransaction: &commonpb.CreatedTransaction{
										Transaction: commonpb.NewTransaction().
											WithPostings(
												commonpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
											).
											WithID(1).
											WithTimestamp(now),
									},
								},
							}).WithID(1).WithDate(now),
						},
					},
				},
			},
			{
				Sequence: 3,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_DeleteLedger{
						DeleteLedger: &commonpb.DeleteLedgerLog{
							Info: &commonpb.LedgerInfo{
								Name:      "new-ledger",
								DeletedAt: commonpb.NewTimestamp(now),
							},
						},
					},
				},
			},
		}
		appendLogs(t, s, 1, mixedLogs...)

		cursor, err := query.ReadLogsSince(context.Background(), s,0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 3)

		// Verify payload types are preserved
		require.NotNil(t, logs[0].Payload.GetCreateLedger())
		require.NotNil(t, logs[1].Payload.GetApply())
		require.NotNil(t, logs[2].Payload.GetDeleteLedger())
	})
}
