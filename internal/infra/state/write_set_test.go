package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
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
	info, err := buf.Ledgers().Get(domain.LedgerKey{Name: "nonexistent"})
	require.ErrorIs(t, err, domain.ErrNotFound)
	require.Nil(t, info)

	// Put and get
	buf.Ledgers().Put(domain.LedgerKey{Name: "test"}, &commonpb.LedgerInfo{Name: "test"})
	info, err = buf.Ledgers().Get(domain.LedgerKey{Name: "test"})
	require.NoError(t, err)
	require.Equal(t, "test", info.GetName())
}

func TestWriteSetGetPutBoundaries(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	// Non-existent
	b, err := buf.Boundaries().Get(domain.LedgerKey{Name: "nonexistent"})
	require.ErrorIs(t, err, domain.ErrNotFound)
	require.Nil(t, b)

	// Put and get
	buf.Boundaries().Put(domain.LedgerKey{Name: "ledger-1"}, &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 10,
		NextLogId:         20,
	})
	b, err = buf.Boundaries().Get(domain.LedgerKey{Name: "ledger-1"})
	require.NoError(t, err)
	require.Equal(t, uint64(10), b.GetNextTransactionId())
	require.Equal(t, uint64(20), b.GetNextLogId())
}

func TestWriteSetGetPutAccountMetadata(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	key := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"}, Key: "role"}

	// Non-existent key falls through to KeyStore which returns ErrNotFound
	_, err := buf.AccountMetadata().Get(key)
	require.ErrorIs(t, err, domain.ErrNotFound)

	buf.AccountMetadata().Put(key, commonpb.NewStringValue("admin"))
	val, err := buf.AccountMetadata().Get(key)
	require.NoError(t, err)
	require.NotNil(t, val)
}

// TestResolveNumscriptContent_AbsentReturnsNilNilNotError pins the EN-1378
// contract for the NumscriptContent reader: a declared-but-absent key returns
// (nil, nil), not (nil, ErrNotFound). processCreateTransaction relies on
// `info == nil` to surface ErrNumscriptNotFound (business error); a propagated
// ErrNotFound would be wrapped as ErrStorageOperation (infra error) and
// misclassify the failure mode for any proposal that references a script the
// proposer's chart never declared.
//
// Caught by NumaryBot review on PR #573 — the same Declare-on-absent shift
// the rest of the PR makes for Volume / NumscriptVersion / PreparedQuery
// applied to NumscriptContent too, and this reader had not been ported to
// explicit ErrNotFound handling yet.
func TestResolveNumscriptContent_AbsentReturnsNilNilNotError(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	info, err := buf.ResolveNumscriptContent("test-ledger", "missing", "v1")
	require.NoError(t, err, "absent NumscriptContent must not surface ErrNotFound to the caller")
	require.Nil(t, info)

	// Put then re-read — the same reader must now return the stored content.
	buf.PutNumscript("test-ledger", &commonpb.NumscriptInfo{
		Name:    "saved",
		Version: "v1",
		Content: "send [USD 1] (source = @world allocating { 1 to @bob })",
		Ledger:  "test-ledger",
	})

	info, err = buf.ResolveNumscriptContent("test-ledger", "saved", "v1")
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, "send [USD 1] (source = @world allocating { 1 to @bob })", info.GetContent())
}

func TestWriteSetDeleteAccountMetadata(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	key := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "bob"}, Key: "label"}
	buf.AccountMetadata().Put(key, commonpb.NewStringValue("value"))

	val, err := buf.AccountMetadata().Get(key)
	require.NoError(t, err)
	require.NotNil(t, val)

	require.NoError(t, buf.AccountMetadata().Delete(key))

	// After delete the key reads as absent (ErrNotFound), like a committed tombstone.
	_, err = buf.AccountMetadata().Get(key)
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

	buf.PutIdempotencyKey(key, &commonpb.IdempotencyKeyValue{FirstLogSequence: 5, LogCount: 1})
	val, err := buf.GetIdempotencyKey(key)
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Equal(t, uint64(5), val.GetFirstLogSequence())
}

func TestWriteSetGetPutTransactionReference(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	key := domain.TransactionReferenceKey{LedgerName: "test", Reference: "ref-1"}

	// Non-existent key returns ErrNotFound
	_, err := buf.TransactionReferences().Get(key)
	require.ErrorIs(t, err, domain.ErrNotFound)

	buf.TransactionReferences().Put(key, &commonpb.TransactionReferenceValue{TransactionId: 100})
	val, err := buf.TransactionReferences().Get(key)
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

	buf.TransactionStates().Put(key, state)
	got, err := buf.TransactionStates().Get(key)
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

	require.Nil(t, buf.chapterScheduleUpdate)

	buf.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetChapterSchedule{SetChapterSchedule: &commonpb.SetChapterScheduleLog{Cron: "*/5 * * * *"}},
	}})
	require.NotNil(t, buf.chapterScheduleUpdate)
	require.Equal(t, "*/5 * * * *", *buf.chapterScheduleUpdate)

	buf.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeleteChapterSchedule{DeleteChapterSchedule: &commonpb.DeletedChapterScheduleLog{}},
	}})
	require.NotNil(t, buf.chapterScheduleUpdate)
	require.Empty(t, *buf.chapterScheduleUpdate)
}

func TestWriteSetSinkConfigOperations(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	// Initially no pending changes
	require.False(t, buf.SinkConfigChanged())

	// Get non-existent
	cfg, err := buf.GetSinkConfig("none")
	require.NoError(t, err)
	require.Nil(t, cfg)

	// Add a config via its log payload.
	buf.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_AddedEventsSink{AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: &commonpb.SinkConfig{Name: "my-sink"}}},
	}})
	require.True(t, buf.SinkConfigChanged())

	cfg, err = buf.GetSinkConfig("my-sink")
	require.NoError(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "my-sink", cfg.GetName())

	// Remove it.
	buf.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RemovedEventsSink{RemovedEventsSink: &commonpb.RemovedEventsSinkLog{Name: "my-sink"}},
	}})
	require.True(t, buf.SinkConfigChanged())
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

func confirmArchiveLog(id, startSeq, closeSeq, startAudit, closeAudit uint64) *commonpb.Log {
	return &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ConfirmArchiveChapter{ConfirmArchiveChapter: &commonpb.ConfirmedArchiveChapterLog{
			Chapter: &commonpb.Chapter{
				Id:                 id,
				StartSequence:      startSeq,
				CloseSequence:      closeSeq,
				StartAuditSequence: startAudit,
				CloseAuditSequence: closeAudit,
			},
		}},
	}}
}

func archiveChapterLog(id, startSeq, closeSeq, startAudit, closeAudit uint64) *commonpb.Log {
	return &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_ArchiveChapter{ArchiveChapter: &commonpb.ArchivedChapterLog{
			Chapter: &commonpb.Chapter{
				Id:                 id,
				StartSequence:      startSeq,
				CloseSequence:      closeSeq,
				StartAuditSequence: startAudit,
				CloseAuditSequence: closeAudit,
			},
		}},
	}}
}

func TestWriteSetSetPurgeRange(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	require.Empty(t, buf.purgeRanges)
	require.False(t, (len(buf.purgeRanges) > 0))

	buf.Absorb(&raftcmdpb.Order{}, confirmArchiveLog(1, 10, 50, 5, 25))
	require.True(t, (len(buf.purgeRanges) > 0))

	buf.Absorb(&raftcmdpb.Order{}, confirmArchiveLog(2, 51, 100, 26, 50))
	require.Len(t, buf.purgeRanges, 2)
	require.Equal(t, uint64(10), buf.purgeRanges[0].startSequence)
	require.Equal(t, uint64(51), buf.purgeRanges[1].startSequence)
	require.True(t, (len(buf.purgeRanges) > 0))
}

func TestWriteSetSetPendingArchive(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	require.Empty(t, buf.archiveRequests)
	buf.Absorb(&raftcmdpb.Order{}, archiveChapterLog(1, 10, 50, 5, 25))
	require.Len(t, buf.archiveRequests, 1)
	require.Equal(t, uint64(1), buf.archiveRequests[0].ChapterID)
	require.Equal(t, uint64(10), buf.archiveRequests[0].StartSequence)
	require.Equal(t, uint64(50), buf.archiveRequests[0].CloseSequence)
	require.Equal(t, uint64(5), buf.archiveRequests[0].StartAuditSequence)
	require.Equal(t, uint64(25), buf.archiveRequests[0].CloseAuditSequence)
}

// TestWriteSetResetIsolation verifies that data written during proposal N is
// not visible after Reset() prepares the WriteSet for proposal N+1.
func TestWriteSetResetIsolation(t *testing.T) {
	t.Parallel()
	buf, _, _ := newTestBuffer(t)

	// --- Proposal N: write various data ---

	// Derived stores
	buf.Ledgers().Put(domain.LedgerKey{Name: "leaked"}, &commonpb.LedgerInfo{Name: "leaked"})
	buf.Boundaries().Put(domain.LedgerKey{Name: "leaked"}, &raftcmdpb.LedgerBoundaries{NextTransactionId: 99})
	buf.AccountMetadata().Put(
		domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"}, Key: "role"},
		commonpb.NewStringValue("admin"),
	)
	buf.PutIdempotencyKey(
		domain.IdempotencyKey{Key: "ik-leak"},
		&commonpb.IdempotencyKeyValue{FirstLogSequence: 7, LogCount: 1},
	)
	buf.TransactionReferences().Put(
		domain.TransactionReferenceKey{LedgerName: "test", Reference: "ref-leak"},
		&commonpb.TransactionReferenceValue{TransactionId: 42},
	)

	// Pending slices
	buf.AddSigningKey("key-leak", []byte("pub"), "")
	buf.SetMaintenanceMode(true)
	buf.SetRequireSignatures(true)
	buf.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetChapterSchedule{SetChapterSchedule: &commonpb.SetChapterScheduleLog{Cron: "*/5 * * * *"}},
	}})
	buf.Absorb(&raftcmdpb.Order{}, confirmArchiveLog(1, 10, 50, 5, 25))
	buf.Absorb(&raftcmdpb.Order{}, archiveChapterLog(1, 10, 50, 5, 25))
	buf.QueueMirrorSync(MirrorSyncWrite{LedgerName: "leaked", Cursor: 42, ClearError: true})

	// Verify data is present before Reset
	_, err := buf.Ledgers().Get(domain.LedgerKey{Name: "leaked"})
	require.NoError(t, err, "ledger should exist before Reset")
	require.True(t, (len(buf.purgeRanges) > 0), "purges should exist before Reset")
	require.Len(t, buf.pendingSigningKeyUpdates, 1)
	require.NotNil(t, buf.pendingMaintenanceModeUpdate)
	require.NotNil(t, buf.pendingSigningConfigUpdate)
	require.NotNil(t, buf.chapterScheduleUpdate)
	require.Len(t, buf.pendingMirrorSyncs, 1)

	// --- Reset for proposal N+1 ---
	buf.Reset(&commonpb.Timestamp{Data: 1700000001})

	// --- Verify complete isolation ---

	// Derived stores must be empty
	_, err = buf.Ledgers().Get(domain.LedgerKey{Name: "leaked"})
	require.ErrorIs(t, err, domain.ErrNotFound, "ledger from previous proposal must not be visible after Reset")

	_, err = buf.Boundaries().Get(domain.LedgerKey{Name: "leaked"})
	require.ErrorIs(t, err, domain.ErrNotFound, "boundaries from previous proposal must not be visible after Reset")

	_, err = buf.AccountMetadata().Get(domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "test", Account: "alice"}, Key: "role"})
	require.ErrorIs(t, err, domain.ErrNotFound, "account metadata from previous proposal must not be visible after Reset")

	_, err = buf.GetIdempotencyKey(domain.IdempotencyKey{Key: "ik-leak"})
	require.ErrorIs(t, err, domain.ErrNotFound, "idempotency key from previous proposal must not be visible after Reset")

	_, err = buf.TransactionReferences().Get(domain.TransactionReferenceKey{LedgerName: "test", Reference: "ref-leak"})
	require.ErrorIs(t, err, domain.ErrNotFound, "transaction reference from previous proposal must not be visible after Reset")

	// Pending slices must be cleared
	require.Empty(t, buf.pendingSigningKeyUpdates, "signing key updates must be cleared after Reset")
	require.Nil(t, buf.pendingMaintenanceModeUpdate, "maintenance mode update must be nil after Reset")
	require.Nil(t, buf.pendingSigningConfigUpdate, "signing config update must be nil after Reset")
	require.Nil(t, buf.chapterScheduleUpdate, "chapter schedule update must be nil after Reset")
	require.False(t, (len(buf.purgeRanges) > 0), "purges must be cleared after Reset")
	require.Empty(t, buf.archiveRequests, "archives must be cleared after Reset")
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

// TestWriteSetPreparedQueryPersistsThroughMerge pins that a prepared query
// committed through buffer.Merge is written to the attributes zone and is
// readable afterwards. It guards that routing prepared queries through
// mergeAndTrackBloom — which now also collects their canonical keys into
// bloomUpdates.PreparedQueries — leaves attribute persistence unaffected. The
// bloom wire itself (keys actually landing in the filter) is exercised by
// TestWriteSetPreparedQueryBloomFilterTracksKeys.
func TestWriteSetPreparedQueryPersistsThroughMerge(t *testing.T) {
	t.Parallel()
	buf, _, dataStore := newTestBuffer(t)

	const ledger = "test"
	buf.PreparedQueries().Put(domain.PreparedQueryKey{LedgerName: ledger, Name: "pq-1"}, &commonpb.PreparedQuery{Name: "pq-1"})

	batch := dataStore.OpenWriteSession()
	require.NoError(t, buf.Merge(batch, nil))
	require.NoError(t, batch.Commit())

	rh, err := dataStore.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rh.Close() })

	pq, err := query.ReadPreparedQuery(context.Background(), buf.attrs.PreparedQuery, rh, ledger, "pq-1")
	require.NoError(t, err)
	require.NotNil(t, pq, "prepared query must persist through Merge")
	require.Equal(t, "pq-1", pq.GetName())
}

// TestWriteSetPreparedQueryDeletePersistsThroughMerge pins that deleting a
// committed prepared query in a later proposal removes it from the attributes
// zone, exercising the deletion branch of mergeAndTrackBloom for prepared
// queries. This is a persistence guard; the bloom-filter wire is covered by
// TestWriteSetPreparedQueryBloomFilterTracksKeys.
func TestWriteSetPreparedQueryDeletePersistsThroughMerge(t *testing.T) {
	t.Parallel()
	buf, machine, dataStore := newTestBuffer(t)

	const ledger = "test"

	// Proposal 1: commit a prepared query.
	buf.PreparedQueries().Put(domain.PreparedQueryKey{LedgerName: ledger, Name: "pq-del"}, &commonpb.PreparedQuery{Name: "pq-del"})
	batch := dataStore.OpenWriteSession()
	require.NoError(t, buf.Merge(batch, nil))
	require.NoError(t, batch.Commit())

	// Confirm it is readable before we delete it, so this test fails if the
	// create silently no-ops rather than only when the delete misbehaves.
	rh1, err := dataStore.NewReadHandle()
	require.NoError(t, err)
	pq1, err := query.ReadPreparedQuery(context.Background(), buf.attrs.PreparedQuery, rh1, ledger, "pq-del")
	require.NoError(t, err)
	require.NotNil(t, pq1, "prepared query must exist before deletion")
	require.NoError(t, rh1.Close())

	// Proposal 2: delete it.
	buf2 := NewWriteSet(machine)
	buf2.Reset(&commonpb.Timestamp{Data: 1700000001})
	require.NoError(t, buf2.PreparedQueries().Delete(domain.PreparedQueryKey{LedgerName: ledger, Name: "pq-del"}))
	batch2 := dataStore.OpenWriteSession()
	require.NoError(t, buf2.Merge(batch2, nil))
	require.NoError(t, batch2.Commit())

	rh, err := dataStore.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rh.Close() })

	pq, err := query.ReadPreparedQuery(context.Background(), buf2.attrs.PreparedQuery, rh, ledger, "pq-del")
	require.NoError(t, err)
	require.Nil(t, pq, "deleted prepared query must be absent after Merge")
}

// TestWriteSetPreparedQueryBloomFilterTracksKeys is the regression guard for the
// EN-1321 wiring: it proves a prepared-query canonical key collected during
// Merge actually lands in the prepared-query bloom filter (iso with the other
// attribute types). Without the wire — a filterSnapshot.PreparedQuery field, the
// bloomTypes() descriptor, and the AddCanonicalKeys insert — the filter would be
// nil or never see the key, which the persistence-only tests above would not
// catch.
func TestWriteSetPreparedQueryBloomFilterTracksKeys(t *testing.T) {
	t.Parallel()
	buf, _, dataStore := newTestBuffer(t)

	// Build a FilterSet with prepared queries enabled, exactly as an operator
	// configuring --bloom-prepared-queries-expected-keys would. A non-zero
	// ExpectedKeys is what makes bloomTypes()/Rebuild allocate the filter.
	meter := noop.NewMeterProvider().Meter("test")
	filters := bloom.NewFilterSet(&commonpb.ClusterConfig{
		BloomPreparedQueries: &commonpb.BloomTypeConfig{ExpectedKeys: 1000, FpRate: 0.01},
	}, meter)
	require.NotNil(t, filters)

	pqFilter := filters.FilterForAttrType(dal.SubAttrPreparedQuery)
	require.NotNil(t, pqFilter, "prepared-query filter must be built when configured")

	const ledger = "test"
	buf.PreparedQueries().Put(domain.PreparedQueryKey{LedgerName: ledger, Name: "pq-bloom"}, &commonpb.PreparedQuery{Name: "pq-bloom"})

	batch := dataStore.OpenWriteSession()
	require.NoError(t, buf.Merge(batch, nil))
	require.NoError(t, batch.Commit())

	// Merge collects the canonical key into the prepared-query bloom slice.
	keys := buf.BloomUpdates().PreparedQueries
	require.Len(t, keys, 1, "Merge must collect exactly one prepared-query canonical key")

	// Before AddCanonicalKeys the freshly built filter knows nothing.
	require.False(t, pqFilter.MayContain(keys[0]), "key must be absent before AddCanonicalKeys")

	// AddCanonicalKeys is what the FSM hot path runs post-Merge; it must route
	// the collected prepared-query keys into the filter.
	filters.AddCanonicalKeys(buf.BloomUpdates())

	require.True(t, pqFilter.MayContain(keys[0]), "inserted prepared-query key must be reported present")

	// A never-inserted key must be reported absent (modulo the configured fp rate,
	// which is ~0 with a single key in a 1000-capacity filter).
	absent := attributes.U128{0xFF, 0xFF, 0xFF, 0xFF}
	require.NotEqual(t, keys[0], absent)
	require.False(t, pqFilter.MayContain(absent), "never-inserted key must be reported absent")
}

// TestCompareVolumeKeys pins the (Account, Asset, Color, LedgerName) precedence
// that ValidateTransientVolumes relies on to pick a deterministic offender and
// avoid forking the audit hash chain (EN-1423). Account dominates, Asset breaks
// ties, Color segregates same-(account, asset) buckets, LedgerName is the final
// tiebreaker; equal keys compare 0.
func TestCompareVolumeKeys(t *testing.T) {
	t.Parallel()

	mk := func(ledger, account, asset string) domain.VolumeKey {
		return domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: ledger, Account: account},
			Asset:      asset,
		}
	}
	mkc := func(ledger, account, asset, color string) domain.VolumeKey {
		return domain.VolumeKey{
			AccountKey: domain.AccountKey{LedgerName: ledger, Account: account},
			Asset:      asset,
			Color:      color,
		}
	}

	tests := []struct {
		name string
		a, b domain.VolumeKey
		want int
	}{
		{"account dominates asset", mk("l1", "alpha", "USD"), mk("l1", "beta", "EUR"), -1},
		{"account dominates ledger", mk("l9", "alpha", "USD"), mk("l1", "beta", "USD"), -1},
		{"asset breaks account tie", mk("l1", "alpha", "EUR"), mk("l1", "alpha", "USD"), -1},
		{"color breaks asset tie", mkc("l1", "alpha", "USD", ""), mkc("l1", "alpha", "USD", "RED"), -1},
		{"color dominates ledger", mkc("l9", "alpha", "USD", ""), mkc("l1", "alpha", "USD", "RED"), -1},
		{"ledger is final tiebreaker", mk("l1", "alpha", "USD"), mk("l3", "alpha", "USD"), -1},
		{"equal keys compare 0", mk("l1", "alpha", "USD"), mk("l1", "alpha", "USD"), 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, compareVolumeKeys(tc.a, tc.b))
			require.Equal(t, -tc.want, compareVolumeKeys(tc.b, tc.a), "comparison must be antisymmetric")
		})
	}
}

// TestValidateTransientVolumesListsAllOffendersSorted exercises the real
// ValidateTransientVolumes through a gated proposal scope. Every offending
// transient (account, asset) tuple must appear in the returned error, sorted by
// (Account, Asset) and deduplicated across ledgers — never a map-random subset
// or order (EN-1423). The loop guards against a lucky single pass.
func TestValidateTransientVolumesListsAllOffendersSorted(t *testing.T) {
	t.Parallel()

	buf, machine, _ := newTestBuffer(t)

	newLedger := func(name string, id uint32) *commonpb.LedgerInfo {
		return &commonpb.LedgerInfo{
			Name: name,
			Id:   id,
			AccountTypes: map[string]*commonpb.AccountType{
				"staging": {
					Name:        "staging",
					Pattern:     "staging:{id}",
					Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
				},
			},
		}
	}

	ledgers := []*commonpb.LedgerInfo{newLedger("l-a", 1), newLedger("l-b", 2)}
	for _, li := range ledgers {
		_, _, err := machine.Registry.Ledgers.KeyStore().Put(
			(&domain.LedgerKey{Name: li.GetName()}).Bytes(),
			li,
		)
		require.NoError(t, err)
		buf.Derived.Ledgers.Put(domain.LedgerKey{Name: li.GetName()}, li)
	}

	// Offenders across two ledgers. The l-b/staging:a/USD entry shares
	// (account, asset) with the l-a one — a cross-ledger repeat that must dedup
	// to a single entry. The RED-colored staging:a/USD is a distinct bucket and
	// must NOT fuse with its uncolored sibling.
	offenders := []domain.VolumeKey{
		domain.NewVolumeKey("l-a", "staging:z", "USD", ""),
		domain.NewVolumeKey("l-a", "staging:a", "USD", ""),
		domain.NewVolumeKey("l-a", "staging:a", "USD", "RED"),
		domain.NewVolumeKey("l-b", "staging:m", "EUR", ""),
		domain.NewVolumeKey("l-b", "staging:a", "USD", ""),
	}
	// Non-zero balance (input != output) => offending. Reused read-only.
	nonZero := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(200),
		Output: commonpb.NewUint256FromUint64(50),
	}
	for _, k := range offenders {
		buf.Derived.Volumes.Put(k, nonZero)
	}

	// Coverage: declare each ledger key (Ledgers().Get) and each volume key
	// (CheckCoverage before the base-volume read).
	attrPlans := make([]*raftcmdpb.AttributeCoverage, 0, len(ledgers)+len(offenders))
	for _, li := range ledgers {
		lid, _ := attributes.MakeKey((&domain.LedgerKey{Name: li.GetName()}).Bytes())
		attrPlans = append(attrPlans, declareTestPlan(lid, dal.SubAttrLedger))
	}
	for _, k := range offenders {
		vid, _ := attributes.MakeKey(k.Bytes())
		attrPlans = append(attrPlans, declareTestPlan(vid, dal.SubAttrVolume))
	}

	scope, err := NewScopeFactory(
		buf,
		&raftcmdpb.ExecutionPlan{Attributes: attrPlans},
		machine.logger,
		machine.preloadMissCounter,
		1,
	).NewProposalScope()
	require.NoError(t, err)

	// Sorted by (Account, Asset, Color); the l-b/staging:a/USD repeat is deduped
	// out, but staging:a/USD/RED stays as its own offender (color is identity).
	want := []domain.AccountAssetKey{
		{Account: "staging:a", Asset: "USD"},
		{Account: "staging:a", Asset: "USD", Color: "RED"},
		{Account: "staging:m", Asset: "EUR"},
		{Account: "staging:z", Asset: "USD"},
	}

	for i := range 50 {
		describ := buf.ValidateTransientVolumes(scope)
		require.NotNil(t, describ, "iteration %d: expected offenders", i)

		e, ok := describ.(*domain.ErrTransientAccountNonZero)
		require.True(t, ok, "iteration %d: got %T", i, describ)
		require.Equal(t, want, e.Accounts, "iteration %d", i)
	}
}

// TestValidateTransientVolumesStorageFaultTakesPrecedence pins that a
// should-not-happen storage/coverage fault surfaces ahead of any business
// offender: the check could not run correctly for that key, so the aggregated
// ErrTransientAccountNonZero must not mask it. Here one transient volume has no
// declared volume coverage (CheckCoverage fails => ErrStorageOperation) while
// another is a plain non-zero business offender.
func TestValidateTransientVolumesStorageFaultTakesPrecedence(t *testing.T) {
	t.Parallel()

	buf, machine, _ := newTestBuffer(t)

	ledger := &commonpb.LedgerInfo{
		Name: "l-a",
		Id:   1,
		AccountTypes: map[string]*commonpb.AccountType{
			"staging": {
				Name:        "staging",
				Pattern:     "staging:{id}",
				Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
			},
		},
	}
	_, _, err := machine.Registry.Ledgers.KeyStore().Put(
		(&domain.LedgerKey{Name: ledger.GetName()}).Bytes(),
		ledger,
	)
	require.NoError(t, err)
	buf.Derived.Ledgers.Put(domain.LedgerKey{Name: ledger.GetName()}, ledger)

	businessOffender := domain.NewVolumeKey("l-a", "staging:a", "USD", "")
	uncoveredOffender := domain.NewVolumeKey("l-a", "staging:z", "USD", "")
	nonZero := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(200),
		Output: commonpb.NewUint256FromUint64(50),
	}
	buf.Derived.Volumes.Put(businessOffender, nonZero)
	buf.Derived.Volumes.Put(uncoveredOffender, nonZero)

	// Declare the ledger and ONLY the business offender's volume coverage.
	// uncoveredOffender is deliberately left undeclared so its CheckCoverage
	// fails inside ValidateTransientVolumes.
	lid, _ := attributes.MakeKey((&domain.LedgerKey{Name: ledger.GetName()}).Bytes())
	vid, _ := attributes.MakeKey(businessOffender.Bytes())
	attrPlans := []*raftcmdpb.AttributeCoverage{
		declareTestPlan(lid, dal.SubAttrLedger),
		declareTestPlan(vid, dal.SubAttrVolume),
	}

	scope, err := NewScopeFactory(
		buf,
		&raftcmdpb.ExecutionPlan{Attributes: attrPlans},
		machine.logger,
		machine.preloadMissCounter,
		1,
	).NewProposalScope()
	require.NoError(t, err)

	for i := range 50 {
		describ := buf.ValidateTransientVolumes(scope)
		require.NotNil(t, describ, "iteration %d: expected a fault", i)

		_, ok := describ.(*domain.ErrStorageOperation)
		require.True(t, ok, "iteration %d: storage fault must win over business offender, got %T", i, describ)
	}
}
