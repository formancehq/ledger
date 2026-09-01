package state

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func closeChapterOrder() *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
		Payload: &raftcmdpb.SystemScopedOrder_CloseChapter{CloseChapter: &raftcmdpb.CloseChapterOrder{}},
	}}}
}

func sealChapterOrder(id uint64, sealingHash, stateHash []byte) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
		Payload: &raftcmdpb.SystemScopedOrder_SealChapter{SealChapter: &raftcmdpb.SealChapterOrder{
			ChapterId:   id,
			SealingHash: sealingHash,
			StateHash:   stateHash,
		}},
	}}}
}

// persistedChapters reads the chapter rows back out of Pebble, which is what
// store check and every restart see — as opposed to the registry the FSM holds
// in memory.
func persistedChapters(t *testing.T, store *dal.Store) []*commonpb.Chapter {
	t.Helper()

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadChapters(context.Background(), handle)
	require.NoError(t, err)

	var chapters []*commonpb.Chapter

	for {
		chapter, err := cursor.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)
		chapters = append(chapters, chapter)
	}

	return chapters
}

func persistedChapter(t *testing.T, store *dal.Store, id uint64) *commonpb.Chapter {
	t.Helper()

	for _, chapter := range persistedChapters(t, store) {
		if chapter.GetId() == id {
			return chapter
		}
	}

	return nil
}

// sealingHashOf is the decomposition store check verifies: a sealed chapter must
// reproduce its own sealing hash from the fields its row carries.
func sealingHashOf(id, closeSequence uint64, lastAuditHash, stateHash []byte) []byte {
	hasher := blake3.New()
	buf := make([]byte, 8)

	binary.BigEndian.PutUint64(buf, id)
	_, _ = hasher.Write(buf)
	binary.BigEndian.PutUint64(buf, closeSequence)
	_, _ = hasher.Write(buf)

	if len(lastAuditHash) > 0 {
		_, _ = hasher.Write(lastAuditHash)
	}

	_, _ = hasher.Write(stateHash)

	return hasher.Sum(nil)
}

// auditedChapter applies a ledger and a transaction so the chapter closes over a
// non-empty audit chain, then closes it. Returns the closed chapter's id.
func auditedChapter(t *testing.T, machine *Machine, store *dal.Store) uint64 {
	t.Helper()

	ctx := context.Background()

	r, err := machine.ApplyEntries(ctx, store, makeEntry(t, 1, makeProposal(1, createLedgerOrder("anchor"))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	r, err = machine.ApplyEntries(ctx, store, makeEntry(t, 2, makeProposal(2,
		createTransactionOrder("anchor", true, newPosting("world", "alice", "EUR", 100)))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	closing := machine.Chapters.LatestClosingChapter()
	require.Nil(t, closing, "no chapter is closing before the close")

	r, err = machine.ApplyEntries(ctx, store, makeEntry(t, 3, makeProposal(3, closeChapterOrder())))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	closing = machine.Chapters.LatestClosingChapter()
	require.NotNil(t, closing)

	return closing.GetId()
}

// The audit anchor is the chain input store check uses for the first entry that
// survives a chapter's purge, and one of the four fields its sealing hash commits
// to. The close is the only apply that knows it, so the close has to persist it:
// anything filled in afterwards is lost by a restart, and the seal that hashes it
// can be applied by a process that never saw the close.
func TestApplyProposal_CloseChapterPersistsTheAuditAnchor(t *testing.T) {
	t.Parallel()

	machine, store, _ := newTestMachine(t)

	id := auditedChapter(t, machine, store)

	require.NotEmpty(t, machine.Chapters.LatestClosingChapter().GetLastAuditHash(),
		"the registry the sealer reads carries the anchor")

	row := persistedChapter(t, store, id)
	require.NotNil(t, row)
	require.Equal(t, machine.Chapters.LatestClosingChapter().GetLastAuditHash(), row.GetLastAuditHash(),
		"the persisted row must carry the same anchor, not acquire it from a later write")
}

// The production failure: the sealer hashes the anchor from the registry, and the
// seal is applied by a process that reloaded that registry from Pebble. If the
// close did not persist the anchor, the row it writes cannot reproduce the
// sealing hash it stores, and store check reports HASH_MISMATCH on the chapter
// forever.
func TestApplyProposal_SealAfterRecoveryReproducesItsSealingHash(t *testing.T) {
	t.Parallel()

	machine, store, _ := newTestMachine(t)
	ctx := context.Background()

	id := auditedChapter(t, machine, store)

	// What the Sealer does: build the request from the chapter the FSM holds and
	// hash it into the sealing hash it proposes.
	request := SealRequestFromChapter(machine.Chapters.LatestClosingChapter())
	require.NotEmpty(t, request.LastAuditHash, "the sealer hashes the anchor")

	stateHash := []byte("state-hash-standing-in-for-a-fold")
	sealingHash := sealingHashOf(request.ChapterID, request.CloseSequence, request.LastAuditHash, stateHash)

	// The node restarts before the seal applies: the registry comes back from
	// Pebble, carrying only what the close persisted.
	require.NoError(t, NewRecovery(machine, store).RecoverState())

	r, err := machine.ApplyEntries(ctx, store, makeEntry(t, 4, makeProposal(4,
		sealChapterOrder(request.ChapterID, sealingHash, stateHash))))
	require.NoError(t, err)
	require.NoError(t, r.Results[0].Error)

	row := persistedChapter(t, store, id)
	require.NotNil(t, row)
	require.Equal(t, commonpb.ChapterStatus_CHAPTER_CLOSED, row.GetStatus())
	require.Equal(t,
		sealingHashOf(row.GetId(), row.GetCloseSequence(), row.GetLastAuditHash(), row.GetStateHash()),
		row.GetSealingHash(),
		"the sealed row must reproduce its own sealing hash — this is what store check verifies")
}
