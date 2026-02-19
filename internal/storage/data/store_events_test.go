package data_test

import (
	"io"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func newTestStore(t *testing.T) *data.Store {
	t.Helper()
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := data.NewStore(t.TempDir(), logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func collectLogs(t *testing.T, cursor data.Cursor[*commonpb.Log]) []*commonpb.Log {
	t.Helper()
	defer func() { _ = cursor.Close() }()

	var logs []*commonpb.Log
	for {
		log, err := cursor.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		logs = append(logs, log)
	}
	return logs
}

// setSinkCursorViaBatch writes a per-sink events cursor via a batch (as the FSM does).
func setSinkCursorViaBatch(t *testing.T, s *data.Store, sinkName string, sequence uint64) {
	t.Helper()
	batch := s.NewBatch()
	require.NoError(t, batch.SetSinkCursor(sinkName, sequence))
	require.NoError(t, batch.Commit())
}

func TestSinkCursor(t *testing.T) {
	t.Parallel()

	t.Run("InitialCursorIsZero", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		cursor, err := s.GetSinkCursor("my-sink")
		require.NoError(t, err)
		require.Equal(t, uint64(0), cursor)
	})

	t.Run("SetAndGetCursor", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "my-sink", 42)

		cursor, err := s.GetSinkCursor("my-sink")
		require.NoError(t, err)
		require.Equal(t, uint64(42), cursor)
	})

	t.Run("CursorOverwrite", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "my-sink", 10)
		setSinkCursorViaBatch(t, s, "my-sink", 20)

		cursor, err := s.GetSinkCursor("my-sink")
		require.NoError(t, err)
		require.Equal(t, uint64(20), cursor)
	})

	t.Run("CursorPersistsAcrossReads", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "my-sink", 99)

		// Read multiple times to ensure consistency
		for range 3 {
			cursor, err := s.GetSinkCursor("my-sink")
			require.NoError(t, err)
			require.Equal(t, uint64(99), cursor)
		}
	})

	t.Run("IndependentPerSink", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "sink-a", 10)
		setSinkCursorViaBatch(t, s, "sink-b", 20)

		cursorA, err := s.GetSinkCursor("sink-a")
		require.NoError(t, err)
		require.Equal(t, uint64(10), cursorA)

		cursorB, err := s.GetSinkCursor("sink-b")
		require.NoError(t, err)
		require.Equal(t, uint64(20), cursorB)
	})
}

func TestSinkStatus(t *testing.T) {
	t.Parallel()

	t.Run("NilWhenNotSet", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		statuses, err := s.LoadAllSinkStatuses()
		require.NoError(t, err)
		require.Empty(t, statuses)
	})

	t.Run("SaveAndLoad", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		batch := s.NewBatch()
		require.NoError(t, batch.SetSinkStatus(&commonpb.SinkStatus{
			SinkName: "nats-1",
			Cursor:   42,
			Error: &commonpb.SinkError{
				Message:    "connection refused",
				OccurredAt: commonpb.NewTimestamp(libtime.Now()),
			},
		}))
		require.NoError(t, batch.Commit())

		statuses, err := s.LoadAllSinkStatuses()
		require.NoError(t, err)
		require.Len(t, statuses, 1)
		require.Equal(t, "nats-1", statuses[0].SinkName)
		require.Equal(t, uint64(42), statuses[0].Cursor)
		require.Equal(t, "connection refused", statuses[0].Error.Message)
	})

	t.Run("ClearStatus", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		// Set a status
		batch := s.NewBatch()
		require.NoError(t, batch.SetSinkStatus(&commonpb.SinkStatus{
			SinkName: "nats-1",
			Cursor:   10,
		}))
		require.NoError(t, batch.Commit())

		// Clear it
		batch = s.NewBatch()
		require.NoError(t, batch.ClearSinkStatus("nats-1"))
		require.NoError(t, batch.Commit())

		statuses, err := s.LoadAllSinkStatuses()
		require.NoError(t, err)
		require.Empty(t, statuses)
	})

	t.Run("MultipleSinks", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		batch := s.NewBatch()
		require.NoError(t, batch.SetSinkStatus(&commonpb.SinkStatus{
			SinkName: "nats-1",
			Cursor:   10,
		}))
		require.NoError(t, batch.SetSinkStatus(&commonpb.SinkStatus{
			SinkName: "nats-2",
			Cursor:   20,
		}))
		require.NoError(t, batch.Commit())

		statuses, err := s.LoadAllSinkStatuses()
		require.NoError(t, err)
		require.Len(t, statuses, 2)
	})
}

func TestSinkConfig(t *testing.T) {
	t.Parallel()

	t.Run("EmptyWhenNotSet", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		configs, err := s.LoadAllSinkConfigs()
		require.NoError(t, err)
		require.Empty(t, configs)
	})

	t.Run("SaveAndLoadSingle", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		batch := s.NewBatch()
		require.NoError(t, batch.SaveSinkConfig(&commonpb.SinkConfig{
			Name: "primary-nats",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{
					Url:   "nats://localhost:4222",
					Topic: "ledger.events",
				},
			},
			Format:       "json",
			BatchSize:    32,
			BatchDelayMs: 50,
		}))
		require.NoError(t, batch.Commit())

		cfg, err := s.LoadSinkConfig("primary-nats")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "primary-nats", cfg.Name)
		require.Equal(t, "json", cfg.Format)
		require.Equal(t, int32(32), cfg.BatchSize)
		require.Equal(t, int64(50), cfg.BatchDelayMs)
		natsCfg := cfg.GetNats()
		require.NotNil(t, natsCfg)
		require.Equal(t, "nats://localhost:4222", natsCfg.Url)
		require.Equal(t, "ledger.events", natsCfg.Topic)
	})

	t.Run("LoadAllSinkConfigs", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		batch := s.NewBatch()
		require.NoError(t, batch.SaveSinkConfig(&commonpb.SinkConfig{
			Name:   "sink-a",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://a:4222"},
			},
		}))
		require.NoError(t, batch.SaveSinkConfig(&commonpb.SinkConfig{
			Name:   "sink-b",
			Format: "protobuf",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://b:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		configs, err := s.LoadAllSinkConfigs()
		require.NoError(t, err)
		require.Len(t, configs, 2)
	})

	t.Run("DeleteSinkConfig", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		// Save two sinks
		batch := s.NewBatch()
		require.NoError(t, batch.SaveSinkConfig(&commonpb.SinkConfig{
			Name:   "sink-a",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://a:4222"},
			},
		}))
		require.NoError(t, batch.SaveSinkConfig(&commonpb.SinkConfig{
			Name:   "sink-b",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://b:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		// Delete one
		batch = s.NewBatch()
		require.NoError(t, batch.DeleteSinkConfig("sink-a"))
		require.NoError(t, batch.Commit())

		configs, err := s.LoadAllSinkConfigs()
		require.NoError(t, err)
		require.Len(t, configs, 1)
		require.Equal(t, "sink-b", configs[0].Name)

		// Verify the deleted one returns nil
		cfg, err := s.LoadSinkConfig("sink-a")
		require.NoError(t, err)
		require.Nil(t, cfg)
	})

	t.Run("UpsertSinkConfig", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		// Save initial config
		batch := s.NewBatch()
		require.NoError(t, batch.SaveSinkConfig(&commonpb.SinkConfig{
			Name:   "my-sink",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://old:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		// Overwrite with new URL
		batch = s.NewBatch()
		require.NoError(t, batch.SaveSinkConfig(&commonpb.SinkConfig{
			Name:   "my-sink",
			Format: "protobuf",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://new:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		cfg, err := s.LoadSinkConfig("my-sink")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "protobuf", cfg.Format)
		require.Equal(t, "nats://new:4222", cfg.GetNats().Url)

		// Should still be only one config
		configs, err := s.LoadAllSinkConfigs()
		require.NoError(t, err)
		require.Len(t, configs, 1)
	})
}

func TestListLogsSince(t *testing.T) {
	t.Parallel()

	t.Run("EmptyStore", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		cursor, err := s.ListLogsSince(0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("AllLogs", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger", 1)
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// afterSequence=0 should return all logs
		cursor, err := s.ListLogsSince(0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 4)
		require.Equal(t, uint64(1), logs[0].Sequence)
		require.Equal(t, uint64(4), logs[3].Sequence)
	})

	t.Run("LogsAfterSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger", 1)
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// afterSequence=2 should return logs 3 and 4
		cursor, err := s.ListLogsSince(2)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 2)
		require.Equal(t, uint64(3), logs[0].Sequence)
		require.Equal(t, uint64(4), logs[1].Sequence)
	})

	t.Run("LogsAfterLastSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger", 1)
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// afterSequence=4 (last log) should return empty
		cursor, err := s.ListLogsSince(4)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("LogsAfterFarFutureSequence", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger", 1)
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		cursor, err := s.ListLogsSince(999)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Empty(t, logs)
	})

	t.Run("IncrementalRead", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		registerLedger(t, s, "test-ledger", 1)
		testLogs := createTestLogs("test-ledger")
		appendLogs(t, s, 1, testLogs...)

		// Simulate emitter: read all, then read after cursor
		cursor, err := s.ListLogsSince(0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 4)

		lastSeq := logs[len(logs)-1].Sequence

		// Append more logs
		moreLogs := createTestLogsForLedger("test-ledger", 5)
		appendLogs(t, s, 2, moreLogs...)

		// Read only new logs
		cursor, err = s.ListLogsSince(lastSeq)
		require.NoError(t, err)
		newLogs := collectLogs(t, cursor)
		require.Len(t, newLogs, 4) // 4 new logs starting from sequence 5
		require.Equal(t, uint64(5), newLogs[0].Sequence)
	})

	t.Run("LogPayloadTypes", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		now := libtime.Now()
		registerLedger(t, s, "test-ledger", 1)

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
								Id:        2,
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
								Id:        2,
							},
						},
					},
				},
			},
		}
		appendLogs(t, s, 1, mixedLogs...)

		cursor, err := s.ListLogsSince(0)
		require.NoError(t, err)
		logs := collectLogs(t, cursor)
		require.Len(t, logs, 3)

		// Verify payload types are preserved
		require.NotNil(t, logs[0].Payload.GetCreateLedger())
		require.NotNil(t, logs[1].Payload.GetApply())
		require.NotNil(t, logs[2].Payload.GetDeleteLedger())
	})
}
