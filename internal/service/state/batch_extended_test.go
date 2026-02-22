package state

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"
)

func TestSaveMaintenanceMode(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Default: not in maintenance mode
	enabled, err := ReadMaintenanceMode(s)
	require.NoError(t, err)
	require.False(t, enabled)

	// Enable maintenance mode
	batch := s.NewBatch()
	require.NoError(t, SaveMaintenanceMode(batch, true))
	require.NoError(t, batch.Commit())

	enabled, err = ReadMaintenanceMode(s)
	require.NoError(t, err)
	require.True(t, enabled)

	// Disable maintenance mode
	batch = s.NewBatch()
	require.NoError(t, SaveMaintenanceMode(batch, false))
	require.NoError(t, batch.Commit())

	enabled, err = ReadMaintenanceMode(s)
	require.NoError(t, err)
	require.False(t, enabled)
}

func TestSavePeriodSchedule(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Default: empty schedule
	schedule, err := ReadPeriodSchedule(s)
	require.NoError(t, err)
	require.Equal(t, "", schedule)

	// Save a cron expression
	batch := s.NewBatch()
	require.NoError(t, SavePeriodSchedule(batch, "*/5 * * * *"))
	require.NoError(t, batch.Commit())

	schedule, err = ReadPeriodSchedule(s)
	require.NoError(t, err)
	require.Equal(t, "*/5 * * * *", schedule)

	// Update schedule
	batch = s.NewBatch()
	require.NoError(t, SavePeriodSchedule(batch, "0 * * * *"))
	require.NoError(t, batch.Commit())

	schedule, err = ReadPeriodSchedule(s)
	require.NoError(t, err)
	require.Equal(t, "0 * * * *", schedule)
}

func TestBatchDeletePeriodScheduleFunc(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Save a schedule
	batch := s.NewBatch()
	require.NoError(t, SavePeriodSchedule(batch, "*/10 * * * *"))
	require.NoError(t, batch.Commit())

	schedule, err := ReadPeriodSchedule(s)
	require.NoError(t, err)
	require.Equal(t, "*/10 * * * *", schedule)

	// Delete the schedule
	batch = s.NewBatch()
	require.NoError(t, BatchDeletePeriodSchedule(batch))
	require.NoError(t, batch.Commit())

	schedule, err = ReadPeriodSchedule(s)
	require.NoError(t, err)
	require.Equal(t, "", schedule)
}

func TestSaveSinkConfig(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Save a sink config
	config := &commonpb.SinkConfig{
		Name: "my-sink",
		Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "http://example.com"}},
	}
	batch := s.NewBatch()
	require.NoError(t, SaveSinkConfig(batch, config))
	require.NoError(t, batch.Commit())

	// Read it back by iterating over the events config prefix
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixEventsConfig).PutString("my-sink")
	val, closer, err := s.Get(kb.Build())
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	readBack := &commonpb.SinkConfig{}
	require.NoError(t, proto.Unmarshal(val, readBack))
	require.Equal(t, "my-sink", readBack.Name)
}

func TestDeleteSinkConfig(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Save a config
	batch := s.NewBatch()
	require.NoError(t, SaveSinkConfig(batch, &commonpb.SinkConfig{
		Name: "sink-to-delete",
		Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "http://example.com"}},
	}))
	require.NoError(t, batch.Commit())

	// Delete it
	batch = s.NewBatch()
	require.NoError(t, DeleteSinkConfig(batch, "sink-to-delete"))
	require.NoError(t, batch.Commit())

	// Verify it's gone
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixEventsConfig).PutString("sink-to-delete")
	_, _, err := s.Get(kb.Build())
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestSetSinkCursor(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	batch := s.NewBatch()
	require.NoError(t, SetSinkCursor(batch, "my-sink", 42))
	require.NoError(t, batch.Commit())

	// Read back via direct key access
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixSinkCursor).PutString("my-sink")
	val, closer, err := s.Get(kb.Build())
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	require.Len(t, val, 8)
}

func TestSetSinkStatus(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	status := &commonpb.SinkStatus{
		SinkName: "test-sink",
		Cursor:   42,
	}
	batch := s.NewBatch()
	require.NoError(t, SetSinkStatus(batch, status))
	require.NoError(t, batch.Commit())

	// Verify by reading back
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixSinkStatus).PutString("test-sink")
	val, closer, err := s.Get(kb.Build())
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	require.NotEmpty(t, val)
}

func TestClearSinkStatus(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Set a status
	batch := s.NewBatch()
	require.NoError(t, SetSinkStatus(batch, &commonpb.SinkStatus{
		SinkName: "clear-me",
		Cursor:   10,
	}))
	require.NoError(t, batch.Commit())

	// Clear it
	batch = s.NewBatch()
	require.NoError(t, ClearSinkStatus(batch, "clear-me"))
	require.NoError(t, batch.Commit())

	// Verify it's gone
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixSinkStatus).PutString("clear-me")
	_, _, err := s.Get(kb.Build())
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestPurgeTransactionUpdates(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	// Store transaction updates with different byLog values
	batch := s.NewBatch()
	for _, seq := range []uint64{5, 10, 15, 20, 25} {
		key := dal.TransactionKey{LedgerID: 1, ID: 1}
		update := &commonpb.TransactionUpdate{
			ByLog: seq,
			Updates: []*commonpb.TransactionUpdateType{
				{
					TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
						TransactionInit: &commonpb.TransactionInit{},
					},
				},
			},
		}
		require.NoError(t, StoreTransactionUpdate(batch, key, update))
	}
	require.NoError(t, batch.Commit())

	// Verify all 5 updates exist
	updates, err := ReadTransactionUpdates(s, 1, 1)
	require.NoError(t, err)
	require.Len(t, updates, 5)

	// Purge updates with byLog in [10, 20]
	batch = s.NewBatch()
	require.NoError(t, PurgeTransactionUpdates(batch, 10, 20))
	require.NoError(t, batch.Commit())

	// Verify only updates with byLog 5 and 25 remain
	updates, err = ReadTransactionUpdates(s, 1, 1)
	require.NoError(t, err)
	require.Len(t, updates, 2)
	require.Equal(t, uint64(5), updates[0].ByLog)
	require.Equal(t, uint64(25), updates[1].ByLog)
}

func TestAppendAuditEntries(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	entries := []*auditpb.AuditEntry{
		{Sequence: 1, ProposalId: 10, Timestamp: commonpb.NewTimestamp(libtime.Now())},
		{Sequence: 2, ProposalId: 20, Timestamp: commonpb.NewTimestamp(libtime.Now())},
		{Sequence: 3, ProposalId: 30, Timestamp: commonpb.NewTimestamp(libtime.Now())},
	}

	batch := s.NewBatch()
	require.NoError(t, AppendAuditEntries(batch, entries...))
	require.NoError(t, batch.Commit())

	// Verify we can read them back
	lastSeq, err := ReadLastAuditSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(3), lastSeq)

	// Read single entry
	entry, err := ReadAuditEntry(s, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), entry.Sequence)
	require.Equal(t, uint64(20), entry.ProposalId)

	// Read non-existent entry
	_, err = ReadAuditEntry(s, 999)
	require.ErrorIs(t, err, dal.ErrNotFound)
}

func TestSetAppliedIndexAndTimestamp(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	batch := s.NewBatch()
	require.NoError(t, SetAppliedIndex(batch, 42))
	require.NoError(t, SetLastAppliedTimestamp(batch, 1700000000))
	require.NoError(t, batch.Commit())

	idx, err := ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(42), idx)

	ts, err := ReadLastAppliedTimestamp(s)
	require.NoError(t, err)
	require.Equal(t, uint64(1700000000), ts)
}

func TestReadTransactionUpdates(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Store updates for two different transactions
	batch := s.NewBatch()
	require.NoError(t, StoreTransactionUpdate(batch,
		dal.TransactionKey{LedgerID: 1, ID: 100},
		&commonpb.TransactionUpdate{
			ByLog: 1,
			Updates: []*commonpb.TransactionUpdateType{
				{TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
					TransactionInit: &commonpb.TransactionInit{},
				}},
			},
		},
	))
	require.NoError(t, StoreTransactionUpdate(batch,
		dal.TransactionKey{LedgerID: 1, ID: 100},
		&commonpb.TransactionUpdate{
			ByLog: 5,
			Updates: []*commonpb.TransactionUpdateType{
				{TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationAddMetadata{
					TransactionModificationAddMetadata: &commonpb.TransactionUpdateAddMetadata{},
				}},
			},
		},
	))
	require.NoError(t, StoreTransactionUpdate(batch,
		dal.TransactionKey{LedgerID: 1, ID: 200},
		&commonpb.TransactionUpdate{
			ByLog: 2,
			Updates: []*commonpb.TransactionUpdateType{
				{TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
					TransactionInit: &commonpb.TransactionInit{},
				}},
			},
		},
	))
	require.NoError(t, batch.Commit())

	// Read updates for transaction 100
	updates, err := ReadTransactionUpdates(s, 1, 100)
	require.NoError(t, err)
	require.Len(t, updates, 2)
	require.Equal(t, uint64(1), updates[0].ByLog)
	require.Equal(t, uint64(5), updates[1].ByLog)

	// Read updates for transaction 200
	updates, err = ReadTransactionUpdates(s, 1, 200)
	require.NoError(t, err)
	require.Len(t, updates, 1)

	// Read updates for non-existent transaction
	updates, err = ReadTransactionUpdates(s, 1, 999)
	require.NoError(t, err)
	require.Empty(t, updates)
}

func TestFindTransactionCreationLog(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Register a ledger
	registerLedger(t, s, "find-tx-ledger", 1)

	// Store a transaction update with TransactionInit
	batch := s.NewBatch()
	require.NoError(t, StoreTransactionUpdate(batch,
		dal.TransactionKey{LedgerID: 1, ID: 1},
		&commonpb.TransactionUpdate{
			ByLog: 5,
			Updates: []*commonpb.TransactionUpdateType{
				{TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
					TransactionInit: &commonpb.TransactionInit{},
				}},
			},
		},
	))
	require.NoError(t, batch.Commit())

	// Store a log at sequence 5
	logs := []*commonpb.Log{{
		Sequence: 5,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: "find-tx-ledger",
			},
		}},
	}}
	appendLogs(t, s, 1, logs...)

	// Find the creation log
	log, err := FindTransactionCreationLog(s, "find-tx-ledger", 1)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(5), log.Sequence)

	// Non-existent transaction should return ErrNotFound
	_, err = FindTransactionCreationLog(s, "find-tx-ledger", 999)
	require.ErrorIs(t, err, dal.ErrNotFound)

	// Non-existent ledger should return error
	_, err = FindTransactionCreationLog(s, "non-existent", 1)
	require.Error(t, err)
}

func TestReadMaxLedgerID(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Empty store
	maxID, found, err := ReadMaxLedgerID(s)
	require.NoError(t, err)
	require.False(t, found)
	require.Equal(t, uint32(0), maxID)

	// Register ledgers
	registerLedger(t, s, "maxid-a", 5)
	registerLedger(t, s, "maxid-b", 10)
	registerLedger(t, s, "maxid-c", 3)

	maxID, found, err = ReadMaxLedgerID(s)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint32(10), maxID)
}

func TestReadLastAuditSequence(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Empty store
	seq, err := ReadLastAuditSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(0), seq)

	// Add audit entries
	batch := s.NewBatch()
	require.NoError(t, AppendAuditEntries(batch,
		&auditpb.AuditEntry{Sequence: 10, Timestamp: commonpb.NewTimestamp(libtime.Now())},
		&auditpb.AuditEntry{Sequence: 20, Timestamp: commonpb.NewTimestamp(libtime.Now())},
		&auditpb.AuditEntry{Sequence: 30, Timestamp: commonpb.NewTimestamp(libtime.Now())},
	))
	require.NoError(t, batch.Commit())

	seq, err = ReadLastAuditSequence(s)
	require.NoError(t, err)
	require.Equal(t, uint64(30), seq)
}

func TestReadSigningKeysCursorFunc(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Empty store
	cursor, err := ReadSigningKeysCursor(s)
	require.NoError(t, err)
	var keys []*commonpb.SigningKey
	for {
		key, curErr := cursor.Next()
		if curErr != nil {
			break
		}
		keys = append(keys, key)
	}
	_ = cursor.Close()
	require.Empty(t, keys)

	// Add signing keys with parent hierarchy
	pubKey1 := make([]byte, 32)
	pubKey2 := make([]byte, 32)
	for i := range pubKey1 {
		pubKey1[i] = byte(i)
		pubKey2[i] = byte(i + 50)
	}

	batch := s.NewBatch()
	require.NoError(t, SaveSigningKey(batch, "root-key", pubKey1, ""))
	require.NoError(t, SaveSigningKey(batch, "child-key", pubKey2, "root-key"))
	require.NoError(t, batch.Commit())

	cursor, err = ReadSigningKeysCursor(s)
	require.NoError(t, err)
	keys = nil
	for {
		key, curErr := cursor.Next()
		if curErr != nil {
			break
		}
		keys = append(keys, key)
	}
	_ = cursor.Close()
	require.Len(t, keys, 2)

	// Find the child key
	var childKey *commonpb.SigningKey
	for _, k := range keys {
		if k.KeyId == "child-key" {
			childKey = k
			break
		}
	}
	require.NotNil(t, childKey)
	require.Equal(t, "root-key", childKey.ParentKeyId)
}

func TestReadAuditEntry(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Non-existent entry
	_, err := ReadAuditEntry(s, 99)
	require.ErrorIs(t, err, dal.ErrNotFound)

	// Add entry and read back
	batch := s.NewBatch()
	require.NoError(t, AppendAuditEntries(batch,
		&auditpb.AuditEntry{
			Sequence:   42,
			ProposalId: 100,
			Timestamp:  commonpb.NewTimestamp(libtime.Now()),
		},
	))
	require.NoError(t, batch.Commit())

	entry, err := ReadAuditEntry(s, 42)
	require.NoError(t, err)
	require.Equal(t, uint64(42), entry.Sequence)
	require.Equal(t, uint64(100), entry.ProposalId)
}
