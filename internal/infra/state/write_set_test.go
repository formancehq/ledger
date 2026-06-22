package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// newTestBuffer creates a Machine and returns a WriteSet for testing accessor methods.
// The returned *dal.Store is exposed so tests can open write sessions directly,
// since Machine no longer holds a *dal.Store field.
func newTestBuffer(t *testing.T) (*WriteSet, *Machine, *dal.Store) {
	t.Helper()
	machine, dataStore, _ := newTestMachine(t)
	buf := NewWriteSet(machine)
	buf.Reset(&commonpb.Timestamp{Data: 1700000000})

	return buf, machine, dataStore
}

func TestWriteSetGetPutLedger(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	// Non-existent ledger returns ErrNotFound
	info, err := buf.GetLedger("nonexistent")
	require.ErrorIs(t, err, domain.ErrNotFound)
	require.Nil(t, info)

	// Put and get
	buf.PutLedger("test", &commonpb.LedgerInfo{Name: "test"})
	info, err = buf.GetLedger("test")
	require.NoError(t, err)
	require.Equal(t, "test", info.GetName())
}

func TestWriteSetGetPutBoundaries(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	// Non-existent
	b, err := buf.GetBoundaries("nonexistent")
	require.ErrorIs(t, err, domain.ErrNotFound)
	require.Nil(t, b)

	// Put and get
	buf.PutBoundaries("ledger-1", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 10,
		NextLogId:         20,
	})
	b, err = buf.GetBoundaries("ledger-1")
	require.NoError(t, err)
	require.Equal(t, uint64(10), b.GetNextTransactionId())
	require.Equal(t, uint64(20), b.GetNextLogId())
}

func TestWriteSetGetPutAccountMetadata(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	key := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"}, Key: "role"}

	// Non-existent key falls through to KeyStore which returns ErrNotFound
	_, err := buf.GetAccountMetadata(key)
	require.ErrorIs(t, err, domain.ErrNotFound)

	buf.PutAccountMetadata(key, commonpb.NewStringValue("admin"))
	val, err := buf.GetAccountMetadata(key)
	require.NoError(t, err)
	require.NotNil(t, val)
}

func TestWriteSetDeleteAccountMetadata(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	key := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "bob"}, Key: "label"}
	buf.PutAccountMetadata(key, commonpb.NewStringValue("value"))

	val, err := buf.GetAccountMetadata(key)
	require.NoError(t, err)
	require.NotNil(t, val)

	buf.DeleteAccountMetadata(key)

	// After delete the key reads as absent (ErrNotFound), like a committed tombstone.
	_, err = buf.GetAccountMetadata(key)
	require.ErrorIs(t, err, domain.ErrNotFound)
}

func TestWriteSetGetPutReverted(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	key := domain.TransactionKey{LedgerName: "test", ID: 42}

	// Non-existent key returns false (not reverted)
	reverted, err := buf.GetReverted(key)
	require.NoError(t, err)
	require.False(t, reverted)

	buf.PutReverted(key, true)
	reverted, err = buf.GetReverted(key)
	require.NoError(t, err)
	require.True(t, reverted)
}

func TestWriteSetGetPutIdempotencyKey(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

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

func TestWriteSetGetPutTransactionReference(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	key := domain.TransactionReferenceKey{LedgerName: "test", Reference: "ref-1"}

	// Non-existent key returns ErrNotFound
	_, err := buf.GetTransactionReference(key)
	require.ErrorIs(t, err, domain.ErrNotFound)

	buf.PutTransactionReference(key, &commonpb.TransactionReferenceValue{TransactionId: 100})
	val, err := buf.GetTransactionReference(key)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Equal(t, uint64(100), val.GetTransactionId())
}

func TestWriteSetTransactionState(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	key := domain.TransactionKey{LedgerName: "test", ID: 1}
	state := &commonpb.TransactionState{
		CreatedByLog: 5,
	}

	buf.PutTransactionState(key, state)
	got, err := buf.GetTransactionState(key)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(5), got.GetCreatedByLog())
}

func TestWriteSetSigningKeyOperations(t *testing.T) {
	t.Parallel()
	buf, machine, _ := newTestBuffer(t)

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
	buf2 := NewWriteSet(machine)
	buf2.Reset(&commonpb.Timestamp{Data: 1700000000})
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

func TestWriteSetSetRequireSignatures(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	require.Nil(t, buf.pendingSigningConfigUpdate)
	buf.SetRequireSignatures(true)
	require.NotNil(t, buf.pendingSigningConfigUpdate)
	require.True(t, buf.pendingSigningConfigUpdate.requireSignatures)
}

func TestWriteSetSetMaintenanceMode(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	require.Nil(t, buf.pendingMaintenanceModeUpdate)
	buf.SetMaintenanceMode(true)
	require.NotNil(t, buf.pendingMaintenanceModeUpdate)
	require.True(t, buf.pendingMaintenanceModeUpdate.enabled)
}

func TestWriteSetSetDeleteChapterSchedule(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	require.Nil(t, buf.pendingChapterScheduleUpdate)

	buf.SetChapterSchedule("*/5 * * * *")
	require.NotNil(t, buf.pendingChapterScheduleUpdate)
	require.Equal(t, "*/5 * * * *", *buf.pendingChapterScheduleUpdate)

	buf.DeleteChapterSchedule()
	require.NotNil(t, buf.pendingChapterScheduleUpdate)
	require.Empty(t, *buf.pendingChapterScheduleUpdate)
}

func TestWriteSetSinkConfigOperations(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

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

func TestWriteSetSequenceIDOperations(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	// NextSequenceID
	startSeqID := buf.GetNextSequenceID()
	seqID := buf.IncrementNextSequenceID()
	require.Equal(t, startSeqID, seqID)
	require.Equal(t, startSeqID+1, buf.GetNextSequenceID())
}

func TestWriteSetDateAndHash(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	require.Equal(t, uint64(1700000000), buf.GetDate().GetData())
}

func TestWriteSetChapterOperations(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	// Initially no open chapter
	p, ok := buf.GetCurrentOpenChapter()
	require.False(t, ok)
	require.Nil(t, p)

	// No closing chapters
	require.Empty(t, buf.GetClosingChapters())

	// Set current open chapter
	openChapter := &commonpb.Chapter{Id: 1, Status: commonpb.ChapterStatus_CHAPTER_OPEN}
	buf.SetCurrentOpenChapter(openChapter)
	p, ok = buf.GetCurrentOpenChapter()
	require.True(t, ok)
	require.Equal(t, uint64(1), p.GetId())

	// Add closing chapter
	closingChapter := &commonpb.Chapter{Id: 2, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	buf.AddClosingChapter(closingChapter)
	cp, ok := buf.GetClosingChapterByID(2)
	require.True(t, ok)
	require.Equal(t, uint64(2), cp.GetId())
	require.Len(t, buf.GetClosingChapters(), 1)

	// Add a second closing chapter
	closingChapter2 := &commonpb.Chapter{Id: 3, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	buf.AddClosingChapter(closingChapter2)
	require.Len(t, buf.GetClosingChapters(), 2)

	// Remove first closing chapter
	buf.RemoveClosingChapter(2)
	_, ok = buf.GetClosingChapterByID(2)
	require.False(t, ok)
	require.Len(t, buf.GetClosingChapters(), 1)

	// Remove second closing chapter
	buf.RemoveClosingChapter(3)
	require.Empty(t, buf.GetClosingChapters())
}

func TestWriteSetRemoveClosingChapterRecordsChange(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	closingChapter := &commonpb.Chapter{Id: 7, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	buf.AddClosingChapter(closingChapter)
	initialChanges := len(buf.changedChapters)

	buf.RemoveClosingChapter(7)

	// RemoveClosingChapter should record the removed chapter's final state
	require.Greater(t, len(buf.changedChapters), initialChanges)
	found := false
	for _, p := range buf.changedChapters {
		if p.GetId() == 7 {
			found = true

			break
		}
	}
	require.True(t, found, "removed closing chapter should be in changedChapters")
}

func TestWriteSetMultipleClosingChaptersAfterMerge(t *testing.T) {
	t.Parallel()
	buf, machine, dataStore := newTestBuffer(t)

	// Add two closing chapters
	p1 := &commonpb.Chapter{Id: 10, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	p2 := &commonpb.Chapter{Id: 11, Status: commonpb.ChapterStatus_CHAPTER_CLOSING}
	buf.AddClosingChapter(p1)
	buf.AddClosingChapter(p2)

	// After Merge, the machine should have both closing chapters
	batch := dataStore.OpenWriteSession()
	err := buf.Merge(batch, nil)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	require.Len(t, machine.Chapters.ClosingChapters(), 2)
	_, ok := machine.Chapters.ClosingChapterByID(10)
	require.True(t, ok)
	_, ok = machine.Chapters.ClosingChapterByID(11)
	require.True(t, ok)
}

func TestWriteSetGetNextChapterIDAndIncrement(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	startID := buf.GetNextChapterID()
	id := buf.IncrementNextChapterID()
	require.Equal(t, startID, id)
	require.Equal(t, startID+1, buf.GetNextChapterID())
}

func TestWriteSetGetChapterByID(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	// Non-existent chapter
	_, ok := buf.GetChapterByID(999)
	require.False(t, ok)

	// Add via allChapters (simulating Machine state)
	buf.chapters.PutChapter(&commonpb.Chapter{Id: 10, Status: commonpb.ChapterStatus_CHAPTER_CLOSED})

	p, ok := buf.GetChapterByID(10)
	require.True(t, ok)
	require.Equal(t, uint64(10), p.GetId())

	// Changed chapters take priority over allChapters
	buf.changedChapters = append(buf.changedChapters, &commonpb.Chapter{Id: 10, Status: commonpb.ChapterStatus_CHAPTER_OPEN})
	p, ok = buf.GetChapterByID(10)
	require.True(t, ok)
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_OPEN, p.GetStatus())
}

func TestWriteSetUpdateChapter(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	chapter := &commonpb.Chapter{Id: 5, Status: commonpb.ChapterStatus_CHAPTER_CLOSED}
	buf.UpdateChapter(chapter)
	require.Len(t, buf.changedChapters, 1)
	require.Equal(t, uint64(5), buf.changedChapters[0].GetId())
}

func TestWriteSetSetPurgeRange(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	require.Empty(t, buf.purgeRanges)
	require.False(t, buf.HasPurges())

	buf.SetPurgeRange(1, 10, 50, 5, 25)
	require.True(t, buf.HasPurges())

	buf.SetPurgeRange(2, 51, 100, 26, 50)
	require.Len(t, buf.purgeRanges, 2)
	require.Equal(t, uint64(10), buf.purgeRanges[0].startSequence)
	require.Equal(t, uint64(51), buf.purgeRanges[1].startSequence)
	require.True(t, buf.HasPurges())
}

func TestWriteSetSetPendingArchive(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	require.Empty(t, buf.pendingArchives)
	buf.SetPendingArchive(1, 10, 50, 5, 25)
	require.Len(t, buf.pendingArchives, 1)
	require.Equal(t, uint64(1), buf.pendingArchives[0].ChapterID)
	require.Equal(t, uint64(10), buf.pendingArchives[0].StartSequence)
	require.Equal(t, uint64(50), buf.pendingArchives[0].CloseSequence)
	require.Equal(t, uint64(5), buf.pendingArchives[0].StartAuditSequence)
	require.Equal(t, uint64(25), buf.pendingArchives[0].CloseAuditSequence)
}

func TestWriteSetAddMetadataConvertRequest(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	require.Empty(t, buf.MetadataConvertRequests())

	buf.AddMetadataConvertRequest("ledger-1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "email", commonpb.MetadataType_METADATA_TYPE_STRING)
	reqs := buf.MetadataConvertRequests()
	require.Len(t, reqs, 1)
	require.Equal(t, "ledger-1", reqs[0].LedgerName)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, reqs[0].TargetType)
	require.Equal(t, "email", reqs[0].Key)
	require.Equal(t, commonpb.MetadataType_METADATA_TYPE_STRING, reqs[0].Type)
}

// TestWriteSetResetIsolation verifies that data written during proposal N is
// not visible after Reset() prepares the WriteSet for proposal N+1.
func TestWriteSetResetIsolation(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	// --- Proposal N: write various data ---

	// Derived stores
	buf.PutLedger("leaked", &commonpb.LedgerInfo{Name: "leaked"})
	buf.PutBoundaries("leaked", &raftcmdpb.LedgerBoundaries{NextTransactionId: 99})
	buf.PutAccountMetadata(
		domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"}, Key: "role"},
		commonpb.NewStringValue("admin"),
	)
	buf.PutIdempotencyKey(
		domain.IdempotencyKey{Key: "ik-leak"},
		&commonpb.IdempotencyKeyValue{LogSequence: 7},
	)
	buf.PutTransactionReference(
		domain.TransactionReferenceKey{LedgerName: "test", Reference: "ref-leak"},
		&commonpb.TransactionReferenceValue{TransactionId: 42},
	)

	// Pending slices
	buf.AddSigningKey("key-leak", []byte("pub"), "")
	buf.SetMaintenanceMode(true)
	buf.SetRequireSignatures(true)
	buf.SetChapterSchedule("*/5 * * * *")
	buf.SetPurgeRange(1, 10, 50, 5, 25)
	buf.SetPendingArchive(1, 10, 50, 5, 25)
	buf.AddMetadataConvertRequest("ledger-1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "email", commonpb.MetadataType_METADATA_TYPE_STRING)
	buf.QueueMirrorSync(MirrorSyncWrite{LedgerName: "leaked", Cursor: 42, ClearError: true})

	// Verify data is present before Reset
	_, err := buf.GetLedger("leaked")
	require.NoError(t, err, "ledger should exist before Reset")
	require.True(t, buf.HasPurges(), "purges should exist before Reset")
	require.Len(t, buf.pendingSigningKeyUpdates, 1)
	require.NotNil(t, buf.pendingMaintenanceModeUpdate)
	require.NotNil(t, buf.pendingSigningConfigUpdate)
	require.NotNil(t, buf.pendingChapterScheduleUpdate)
	require.Len(t, buf.pendingMirrorSyncs, 1)

	// --- Reset for proposal N+1 ---
	buf.Reset(&commonpb.Timestamp{Data: 1700000001})

	// --- Verify complete isolation ---

	// Derived stores must be empty
	_, err = buf.GetLedger("leaked")
	require.ErrorIs(t, err, domain.ErrNotFound, "ledger from previous proposal must not be visible after Reset")

	_, err = buf.GetBoundaries("leaked")
	require.ErrorIs(t, err, domain.ErrNotFound, "boundaries from previous proposal must not be visible after Reset")

	_, err = buf.GetAccountMetadata(domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"}, Key: "role"})
	require.ErrorIs(t, err, domain.ErrNotFound, "account metadata from previous proposal must not be visible after Reset")

	_, err = buf.GetIdempotencyKey(domain.IdempotencyKey{Key: "ik-leak"})
	require.ErrorIs(t, err, domain.ErrNotFound, "idempotency key from previous proposal must not be visible after Reset")

	_, err = buf.GetTransactionReference(domain.TransactionReferenceKey{LedgerName: "test", Reference: "ref-leak"})
	require.ErrorIs(t, err, domain.ErrNotFound, "transaction reference from previous proposal must not be visible after Reset")

	// Pending slices must be cleared
	require.Empty(t, buf.pendingSigningKeyUpdates, "signing key updates must be cleared after Reset")
	require.Nil(t, buf.pendingMaintenanceModeUpdate, "maintenance mode update must be nil after Reset")
	require.Nil(t, buf.pendingSigningConfigUpdate, "signing config update must be nil after Reset")
	require.Nil(t, buf.pendingChapterScheduleUpdate, "chapter schedule update must be nil after Reset")
	require.False(t, buf.HasPurges(), "purges must be cleared after Reset")
	require.Empty(t, buf.pendingArchives, "archives must be cleared after Reset")
	require.Empty(t, buf.MetadataConvertRequests(), "metadata convert requests must be cleared after Reset")
	require.Empty(t, buf.pendingMirrorSyncs, "mirror syncs must be cleared after Reset")

	// Scalar state must be refreshed
	require.Equal(t, uint64(1700000001), buf.GetDate().GetData(), "date must be updated after Reset")
}

// TestWriteSetMergeDrainsMirrorSyncs pins the gating semantics that motivate
// pendingMirrorSyncs: QueueMirrorSync stages the writes, and only buffer.Merge
// (the commit gate that runs when ProcessOrders + ValidateTransientVolumes
// succeed) actually emits them. Reading back through query.ReadMirrorCursor
// closes the loop end-to-end.
func TestWriteSetMergeDrainsMirrorSyncs(t *testing.T) {
	t.Parallel()
	buf, _, dataStore := newTestBuffer(t)

	buf.QueueMirrorSync(MirrorSyncWrite{
		LedgerName:     "test",
		Cursor:         99,
		SourceLogCount: 120,
		ClearError:     true,
	})

	batch := dataStore.OpenWriteSession()
	require.NoError(t, buf.Merge(batch, nil))
	require.NoError(t, batch.Commit())

	rh, err := dataStore.NewReadHandle()
	require.NoError(t, err)

	t.Cleanup(func() { _ = rh.Close() })

	cursor, err := query.ReadMirrorCursor(rh, "test")
	require.NoError(t, err)
	require.Equal(t, uint64(99), cursor, "cursor must be persisted after Merge")

	head, err := query.ReadMirrorSourceHead(rh, "test")
	require.NoError(t, err)
	require.Equal(t, uint64(120), head, "source head must be persisted after Merge")

	status, err := query.ReadMirrorStatus(rh, "test")
	require.NoError(t, err)
	require.Nil(t, status, "status must be cleared by ClearError")
}
