package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// newTestBuffer creates a Machine and returns a Buffered for testing accessor methods.
func newTestBuffer(t *testing.T) (*Buffered, *Machine) {
	t.Helper()
	machine, _, _ := newTestMachine(t)
	buf := NewBuffer(&commonpb.Timestamp{Data: 1700000000}, machine)

	return buf, machine
}

func TestBufferedGetPutLedger(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	// Non-existent ledger returns nil
	info, ok := buf.GetLedger("nonexistent")
	require.False(t, ok)
	require.Nil(t, info)

	// Put and get
	buf.PutLedger("test", &commonpb.LedgerInfo{Name: "test"})
	info, ok = buf.GetLedger("test")
	require.True(t, ok)
	require.Equal(t, "test", info.GetName())
}

func TestBufferedGetPutBoundaries(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	// Non-existent
	b, ok := buf.GetBoundaries("nonexistent")
	require.False(t, ok)
	require.Nil(t, b)

	// Put and get
	buf.PutBoundaries("ledger-1", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 10,
		NextLogId:         20,
	})
	b, ok = buf.GetBoundaries("ledger-1")
	require.True(t, ok)
	require.Equal(t, uint64(10), b.GetNextTransactionId())
	require.Equal(t, uint64(20), b.GetNextLogId())
}

func TestBufferedGetPutAccountMetadata(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	key := domain.MetadataKey{AccountKey: domain.AccountKey{Ledger: "test", Account: "alice"}, Key: "role"}

	// Non-existent key falls through to KeyStore which returns ErrNotFound
	_, err := buf.GetAccountMetadata(key)
	require.ErrorIs(t, err, domain.ErrNotFound)

	buf.PutAccountMetadata(key, commonpb.NewStringValue("admin"))
	val, err := buf.GetAccountMetadata(key)
	require.NoError(t, err)
	require.NotNil(t, val)
}

func TestBufferedDeleteAccountMetadata(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	key := domain.MetadataKey{AccountKey: domain.AccountKey{Ledger: "test", Account: "bob"}, Key: "label"}
	buf.PutAccountMetadata(key, commonpb.NewStringValue("value"))

	val, err := buf.GetAccountMetadata(key)
	require.NoError(t, err)
	require.NotNil(t, val)

	buf.DeleteAccountMetadata(key)

	// After delete, Get should return the tombstone/nil from the derived store
	val, err = buf.GetAccountMetadata(key)
	require.NoError(t, err)
	require.Nil(t, val)
}

func TestBufferedGetPutReverted(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	key := domain.TransactionKey{Ledger: "test", ID: 42}

	// Non-existent key returns false (not reverted)
	reverted, err := buf.GetReverted(key)
	require.NoError(t, err)
	require.False(t, reverted)

	buf.PutReverted(key, true)
	reverted, err = buf.GetReverted(key)
	require.NoError(t, err)
	require.True(t, reverted)
}

func TestBufferedGetPutIdempotencyKey(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	key := domain.IdempotencyKey{Key: "ik-1"}

	// Non-existent key returns ErrNotFound
	_, err := buf.GetIdempotencyKey(key)
	require.ErrorIs(t, err, domain.ErrNotFound)

	buf.PutIdempotencyKey(key, &commonpb.IdempotencyKeyValue{LogSequence: 5})
	val, err := buf.GetIdempotencyKey(key)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Equal(t, uint64(5), val.GetLogSequence())
}

func TestBufferedGetPutTransactionReference(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	key := domain.TransactionReferenceKey{Ledger: "test", Reference: "ref-1"}

	// Non-existent key returns ErrNotFound
	_, err := buf.GetTransactionReference(key)
	require.ErrorIs(t, err, domain.ErrNotFound)

	buf.PutTransactionReference(key, &commonpb.TransactionReferenceValue{TransactionId: 100})
	val, err := buf.GetTransactionReference(key)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Equal(t, uint64(100), val.GetTransactionId())
}

func TestBufferedTransactionState(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	key := domain.TransactionKey{Ledger: "test", ID: 1}
	state := &commonpb.TransactionState{
		CreatedByLog: 5,
	}

	buf.PutTransactionState(key, state)
	got, err := buf.GetTransactionState(key)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(5), got.GetCreatedByLog())
}

func TestBufferedSigningKeyOperations(t *testing.T) {
	t.Parallel()
	buf, machine := newTestBuffer(t)

	// AddSigningKey queues an addition
	buf.AddSigningKey("key-1", []byte("pub1"), "")
	require.Len(t, buf.pendingSigningKeyUpdates, 1)
	require.False(t, buf.pendingSigningKeyUpdates[0].remove)

	// RemoveSigningKey queues a removal
	buf.RemoveSigningKey("key-1")
	require.Len(t, buf.pendingSigningKeyUpdates, 2)
	require.True(t, buf.pendingSigningKeyUpdates[1].remove)

	// GetSigningKeyChildren uses committed state + pending updates
	// Add a key to the committed state first
	machine.keyStore.AddPublicKey("parent", []byte("pub-parent"), "")
	machine.keyStore.AddPublicKey("child", []byte("pub-child"), "parent")

	// In a fresh buffer, children should come from committed state
	buf2 := NewBuffer(&commonpb.Timestamp{Data: 1700000000}, machine)
	children := buf2.GetSigningKeyChildren("parent")
	require.Contains(t, children, "child")

	// Add a pending child
	buf2.AddSigningKey("child-2", []byte("pub-child-2"), "parent")
	children = buf2.GetSigningKeyChildren("parent")
	require.Contains(t, children, "child")
	require.Contains(t, children, "child-2")

	// Remove "child" and verify it disappears
	buf2.RemoveSigningKey("child")
	children = buf2.GetSigningKeyChildren("parent")
	require.NotContains(t, children, "child")
	require.Contains(t, children, "child-2")
}

func TestBufferedSetRequireSignatures(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	require.Nil(t, buf.pendingSigningConfigUpdate)
	buf.SetRequireSignatures(true)
	require.NotNil(t, buf.pendingSigningConfigUpdate)
	require.True(t, buf.pendingSigningConfigUpdate.requireSignatures)
}

func TestBufferedSetMaintenanceMode(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	require.Nil(t, buf.pendingMaintenanceModeUpdate)
	buf.SetMaintenanceMode(true)
	require.NotNil(t, buf.pendingMaintenanceModeUpdate)
	require.True(t, buf.pendingMaintenanceModeUpdate.enabled)
}

func TestBufferedSetDeletePeriodSchedule(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	require.Nil(t, buf.pendingPeriodScheduleUpdate)

	buf.SetPeriodSchedule("*/5 * * * *")
	require.NotNil(t, buf.pendingPeriodScheduleUpdate)
	require.Equal(t, "*/5 * * * *", *buf.pendingPeriodScheduleUpdate)

	buf.DeletePeriodSchedule()
	require.NotNil(t, buf.pendingPeriodScheduleUpdate)
	require.Empty(t, *buf.pendingPeriodScheduleUpdate)
}

func TestBufferedSinkConfigOperations(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	// Initially no pending changes
	require.False(t, buf.HasPendingSinkChanges())

	// Get non-existent
	cfg, err := buf.GetSinkConfig("none")
	require.NoError(t, err)
	require.Nil(t, cfg)

	// Add a config
	buf.AddSinkConfig(&commonpb.SinkConfig{Name: "my-sink"})
	require.True(t, buf.HasPendingSinkChanges())

	cfg, err = buf.GetSinkConfig("my-sink")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "my-sink", cfg.GetName())

	// Remove it
	buf.RemoveSinkConfig("my-sink")
	require.True(t, buf.HasPendingSinkChanges())
}

func TestBufferedSequenceIDOperations(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	// NextSequenceID
	startSeqID := buf.GetNextSequenceID()
	seqID := buf.IncrementNextSequenceID()
	require.Equal(t, startSeqID, seqID)
	require.Equal(t, startSeqID+1, buf.GetNextSequenceID())
}

func TestBufferedDateAndHash(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	require.Equal(t, uint64(1700000000), buf.GetDate().GetData())

	require.Nil(t, buf.GetLastLogHash())
	buf.SetLastLogHash([]byte("hash123"))
	require.Equal(t, []byte("hash123"), buf.GetLastLogHash())
}

func TestBufferedPeriodOperations(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	// Initially no open period
	p, ok := buf.GetCurrentOpenPeriod()
	require.False(t, ok)
	require.Nil(t, p)

	// No closing period
	p, ok = buf.GetClosingPeriod()
	require.False(t, ok)
	require.Nil(t, p)

	// Set current open period
	openPeriod := &commonpb.Period{Id: 1, Status: commonpb.PeriodStatus_PERIOD_OPEN}
	buf.SetCurrentOpenPeriod(openPeriod)
	p, ok = buf.GetCurrentOpenPeriod()
	require.True(t, ok)
	require.Equal(t, uint64(1), p.GetId())

	// Set closing period
	closingPeriod := &commonpb.Period{Id: 2, Status: commonpb.PeriodStatus_PERIOD_CLOSING}
	buf.SetClosingPeriod(closingPeriod)
	p, ok = buf.GetClosingPeriod()
	require.True(t, ok)
	require.Equal(t, uint64(2), p.GetId())

	// Clear closing period
	buf.ClearClosingPeriod()
	p, ok = buf.GetClosingPeriod()
	require.False(t, ok)
	require.Nil(t, p)
}

func TestBufferedGetNextPeriodIDAndIncrement(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	startID := buf.GetNextPeriodID()
	id := buf.IncrementNextPeriodID()
	require.Equal(t, startID, id)
	require.Equal(t, startID+1, buf.GetNextPeriodID())
}

func TestBufferedGetPeriodByID(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	// Non-existent period
	_, ok := buf.GetPeriodByID(999)
	require.False(t, ok)

	// Add via allPeriods (simulating Machine state)
	buf.periods.PutPeriod(&commonpb.Period{Id: 10, Status: commonpb.PeriodStatus_PERIOD_CLOSED})

	p, ok := buf.GetPeriodByID(10)
	require.True(t, ok)
	require.Equal(t, uint64(10), p.GetId())

	// Changed periods take priority over allPeriods
	buf.changedPeriods = append(buf.changedPeriods, &commonpb.Period{Id: 10, Status: commonpb.PeriodStatus_PERIOD_OPEN})
	p, ok = buf.GetPeriodByID(10)
	require.True(t, ok)
	require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, p.GetStatus())
}

func TestBufferedUpdatePeriod(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	period := &commonpb.Period{Id: 5, Status: commonpb.PeriodStatus_PERIOD_CLOSED}
	buf.UpdatePeriod(period)
	require.Len(t, buf.changedPeriods, 1)
	require.Equal(t, uint64(5), buf.changedPeriods[0].GetId())
}

func TestBufferedSetPurgeRange(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	require.Empty(t, buf.purgeRanges)
	require.False(t, buf.HasPurges())

	buf.SetPurgeRange(1, 10, 50)
	require.True(t, buf.HasPurges())

	buf.SetPurgeRange(2, 51, 100)
	require.Len(t, buf.purgeRanges, 2)
	require.Equal(t, uint64(10), buf.purgeRanges[0].startSequence)
	require.Equal(t, uint64(51), buf.purgeRanges[1].startSequence)
	require.True(t, buf.HasPurges())
}

func TestBufferedSetPendingArchive(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	require.Empty(t, buf.pendingArchives)
	buf.SetPendingArchive(1, 10, 50)
	require.Len(t, buf.pendingArchives, 1)
	require.Equal(t, uint64(1), buf.pendingArchives[0].PeriodID)
	require.Equal(t, uint64(10), buf.pendingArchives[0].StartSequence)
	require.Equal(t, uint64(50), buf.pendingArchives[0].CloseSequence)
}

func TestBufferedAddMetadataConvertRequest(t *testing.T) {
	t.Parallel()
	buf, _ := newTestBuffer(t)

	require.Empty(t, buf.MetadataConvertRequests())

	buf.AddMetadataConvertRequest("ledger-1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "email", commonpb.MetadataType_METADATA_TYPE_STRING)
	reqs := buf.MetadataConvertRequests()
	require.Len(t, reqs, 1)
	require.Equal(t, "ledger-1", reqs[0].LedgerName)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, reqs[0].TargetType)
	require.Equal(t, "email", reqs[0].Key)
	require.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, reqs[0].Type)
}
