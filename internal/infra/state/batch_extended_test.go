package state

import (
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestSaveMaintenanceMode(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Default: not in maintenance mode
	enabled, err := query.ReadMaintenanceMode(s)
	require.NoError(t, err)
	require.False(t, enabled)

	// Enable maintenance mode
	batch := s.OpenWriteSession()
	require.NoError(t, SaveMaintenanceMode(batch, true))
	require.NoError(t, batch.Commit())

	enabled, err = query.ReadMaintenanceMode(s)
	require.NoError(t, err)
	require.True(t, enabled)

	// Disable maintenance mode
	batch = s.OpenWriteSession()
	require.NoError(t, SaveMaintenanceMode(batch, false))
	require.NoError(t, batch.Commit())

	enabled, err = query.ReadMaintenanceMode(s)
	require.NoError(t, err)
	require.False(t, enabled)
}

func TestSaveChapterSchedule(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Default: empty schedule
	schedule, err := query.ReadChapterSchedule(s)
	require.NoError(t, err)
	require.Empty(t, schedule)

	// Save a cron expression
	batch := s.OpenWriteSession()
	require.NoError(t, SaveChapterSchedule(batch, "*/5 * * * *"))
	require.NoError(t, batch.Commit())

	schedule, err = query.ReadChapterSchedule(s)
	require.NoError(t, err)
	require.Equal(t, "*/5 * * * *", schedule)

	// Update schedule
	batch = s.OpenWriteSession()
	require.NoError(t, SaveChapterSchedule(batch, "0 * * * *"))
	require.NoError(t, batch.Commit())

	schedule, err = query.ReadChapterSchedule(s)
	require.NoError(t, err)
	require.Equal(t, "0 * * * *", schedule)
}

func TestBatchDeleteChapterScheduleFunc(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Save a schedule
	batch := s.OpenWriteSession()
	require.NoError(t, SaveChapterSchedule(batch, "*/10 * * * *"))
	require.NoError(t, batch.Commit())

	schedule, err := query.ReadChapterSchedule(s)
	require.NoError(t, err)
	require.Equal(t, "*/10 * * * *", schedule)

	// Delete the schedule
	batch = s.OpenWriteSession()
	require.NoError(t, batchDeleteChapterSchedule(batch))
	require.NoError(t, batch.Commit())

	schedule, err = query.ReadChapterSchedule(s)
	require.NoError(t, err)
	require.Empty(t, schedule)
}

func TestSaveSinkConfig(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	attr := attributes.NewAttribute[*commonpb.SinkConfig](dal.SubAttrSinkConfig)

	// Save a sink config via attribute
	config := &commonpb.SinkConfig{
		Name: "my-sink",
		Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "http://example.com"}},
	}
	batch := s.OpenWriteSession()
	_, err := attr.Set(batch, domain.SinkConfigKey{Name: "my-sink"}.Bytes(), config)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Read it back via attribute
	readBack, err := attr.Get(s, domain.SinkConfigKey{Name: "my-sink"}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, readBack)
	require.Equal(t, "my-sink", readBack.GetName())
}

func TestDeleteSinkConfig(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	attr := attributes.NewAttribute[*commonpb.SinkConfig](dal.SubAttrSinkConfig)

	// Save a config
	batch := s.OpenWriteSession()
	_, err := attr.Set(batch, domain.SinkConfigKey{Name: "sink-to-delete"}.Bytes(), &commonpb.SinkConfig{
		Name: "sink-to-delete",
		Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "http://example.com"}},
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Delete it
	batch = s.OpenWriteSession()
	require.NoError(t, attr.Delete(batch, domain.SinkConfigKey{Name: "sink-to-delete"}.Bytes()))
	require.NoError(t, batch.Commit())

	// Verify it's gone
	readBack, err := attr.Get(s, domain.SinkConfigKey{Name: "sink-to-delete"}.Bytes())
	require.NoError(t, err)
	require.Nil(t, readBack)
}

func TestSetSinkCursor(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	batch := s.OpenWriteSession()
	require.NoError(t, SetSinkCursor(batch, "my-sink", 42))
	require.NoError(t, batch.Commit())

	// Read back via direct key access
	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSinkCursor).PutString("my-sink")
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
	batch := s.OpenWriteSession()
	require.NoError(t, SetSinkStatus(batch, status))
	require.NoError(t, batch.Commit())

	// Verify by reading back
	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSinkStatus).PutString("test-sink")
	val, closer, err := s.Get(kb.Build())
	require.NoError(t, err)

	defer func() { _ = closer.Close() }()

	require.NotEmpty(t, val)
}

func TestClearSinkStatus(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Set a status
	batch := s.OpenWriteSession()
	require.NoError(t, SetSinkStatus(batch, &commonpb.SinkStatus{
		SinkName: "clear-me",
		Cursor:   10,
	}))
	require.NoError(t, batch.Commit())

	// Clear it
	batch = s.OpenWriteSession()
	require.NoError(t, ClearSinkStatus(batch, "clear-me"))
	require.NoError(t, batch.Commit())

	// Verify it's gone
	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSinkStatus).PutString("clear-me")
	_, _, err := s.Get(kb.Build())
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func Test_appendAuditEntries(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	entries := []*auditpb.AuditEntry{
		{Sequence: 1, ProposalId: 10, Timestamp: uint64(commonpb.NewTimestamp(libtime.Now()))},
		{Sequence: 2, ProposalId: 20, Timestamp: uint64(commonpb.NewTimestamp(libtime.Now()))},
		{Sequence: 3, ProposalId: 30, Timestamp: uint64(commonpb.NewTimestamp(libtime.Now()))},
	}

	batch := s.OpenWriteSession()
	require.NoError(t, appendAuditEntries(batch, entries...))
	require.NoError(t, batch.Commit())

	// Verify we can read them back
	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	lastSeq, err := query.ReadLastAuditSequence(handle)
	require.NoError(t, err)
	require.Equal(t, uint64(3), lastSeq)

	// Read single entry
	entry, err := query.ReadAuditEntry(context.Background(), s, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), entry.GetSequence())
	require.Equal(t, uint64(20), entry.GetProposalId())

	// Read non-existent entry
	_, err = query.ReadAuditEntry(context.Background(), s, 999)
	require.ErrorIs(t, err, domain.ErrNotFound)
}

func TestSetAppliedIndexAndTimestamp(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	batch := s.OpenWriteSession()
	require.NoError(t, SetAppliedIndex(batch, 42))
	require.NoError(t, setLastAppliedTimestamp(batch, 1700000000))
	require.NoError(t, batch.Commit())

	idx, err := query.ReadLastAppliedIndex(s)
	require.NoError(t, err)
	require.Equal(t, uint64(42), idx)

	ts, err := query.ReadLastAppliedTimestamp(s)
	require.NoError(t, err)
	require.Equal(t, uint64(1700000000), ts)
}

func TestReadTransactionState(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	txAttr := attributes.NewAttribute[*commonpb.TransactionState](dal.SubAttrTransaction)

	// Store state for two different transactions
	batch := s.OpenWriteSession()
	_, err := txAttr.Set(batch,
		domain.TransactionKey{LedgerName: "test", ID: 100}.Bytes(),
		&commonpb.TransactionState{CreatedByLog: 1},
	)
	require.NoError(t, err)
	_, err = txAttr.Set(batch,
		domain.TransactionKey{LedgerName: "test", ID: 200}.Bytes(),
		&commonpb.TransactionState{CreatedByLog: 2},
	)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Read state for transaction 100
	state, err := query.ReadTransactionState(context.Background(), s, txAttr, "test", 100)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(1), state.GetCreatedByLog())

	// Read state for transaction 200
	state, err = query.ReadTransactionState(context.Background(), s, txAttr, "test", 200)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, uint64(2), state.GetCreatedByLog())

	// Read state for non-existent transaction
	state, err = query.ReadTransactionState(context.Background(), s, txAttr, "test", 999)
	require.NoError(t, err)
	require.Nil(t, state)
}

func TestFindTransactionCreationLog(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	txAttr := attributes.NewAttribute[*commonpb.TransactionState](dal.SubAttrTransaction)

	// Register a ledger
	registerLedger(t, s, "find-tx-ledger")

	// Store a transaction state via the attribute system
	batch := s.OpenWriteSession()
	_, err := txAttr.Set(batch,
		domain.TransactionKey{LedgerName: "test", ID: 1}.Bytes(),
		&commonpb.TransactionState{CreatedByLog: 5},
	)
	require.NoError(t, err)
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
	log, err := query.FindTransactionCreationLog(context.Background(), s, txAttr, "test", 1)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(5), log.GetSequence())

	// Non-existent transaction should return ErrNotFound
	_, err = query.FindTransactionCreationLog(context.Background(), s, txAttr, "test", 999)
	require.ErrorIs(t, err, domain.ErrNotFound)

	// Non-existent ledger should return error
	_, err = query.FindTransactionCreationLog(context.Background(), s, txAttr, "other", 1)
	require.Error(t, err)
}

func TestReadLastAuditSequence(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Empty store
	seq, err := query.ReadLastAuditSequence(handle)
	require.NoError(t, err)
	require.Equal(t, uint64(0), seq)

	// Add audit entries
	batch := s.OpenWriteSession()
	require.NoError(t, appendAuditEntries(batch,
		&auditpb.AuditEntry{Sequence: 10, Timestamp: uint64(commonpb.NewTimestamp(libtime.Now()))},
		&auditpb.AuditEntry{Sequence: 20, Timestamp: uint64(commonpb.NewTimestamp(libtime.Now()))},
		&auditpb.AuditEntry{Sequence: 30, Timestamp: uint64(commonpb.NewTimestamp(libtime.Now()))},
	))
	require.NoError(t, batch.Commit())

	seq, err = query.ReadLastAuditSequence(handle)
	require.NoError(t, err)
	require.Equal(t, uint64(30), seq)
}

func TestReadSigningKeysCursorFunc(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Empty store
	cursor, err := query.ReadSigningKeysCursor(context.Background(), handle)
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

	batch := s.OpenWriteSession()
	require.NoError(t, SaveSigningKey(batch, "root-key", pubKey1, ""))
	require.NoError(t, SaveSigningKey(batch, "child-key", pubKey2, "root-key"))
	require.NoError(t, batch.Commit())

	cursor, err = query.ReadSigningKeysCursor(context.Background(), handle)
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
		if k.GetKeyId() == "child-key" {
			childKey = k

			break
		}
	}

	require.NotNil(t, childKey)
	require.Equal(t, "root-key", childKey.GetParentKeyId())
}

func TestReadAuditEntry(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Non-existent entry
	_, err := query.ReadAuditEntry(context.Background(), s, 99)
	require.ErrorIs(t, err, domain.ErrNotFound)

	// Add entry and read back
	batch := s.OpenWriteSession()
	require.NoError(t, appendAuditEntries(batch,
		&auditpb.AuditEntry{
			Sequence:   42,
			ProposalId: 100,
			Timestamp:  uint64(commonpb.NewTimestamp(libtime.Now())),
		},
	))
	require.NoError(t, batch.Commit())

	entry, err := query.ReadAuditEntry(context.Background(), s, 42)
	require.NoError(t, err)
	require.Equal(t, uint64(42), entry.GetSequence())
	require.Equal(t, uint64(100), entry.GetProposalId())
}
