package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAllOrdersAreMaintenanceMode(t *testing.T) {
	t.Parallel()

	t.Run("all maintenance mode", func(t *testing.T) {
		t.Parallel()

		orders := []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{
					Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{
						SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: true},
					},
				},
			}},
			{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{
					Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{
						SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: false},
					},
				},
			}},
		}
		require.True(t, authorizedInMaintenanceMode(orders))
	})

	t.Run("mixed orders", func(t *testing.T) {
		t.Parallel()

		orders := []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{
					Payload: &raftcmdpb.SystemScopedOrder_SetMaintenanceMode{
						SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{Enabled: true},
					},
				},
			}},
			{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "test",
					Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
						CreateLedger: &raftcmdpb.CreateLedgerOrder{},
					},
				},
			}},
		}
		require.False(t, authorizedInMaintenanceMode(orders))
	})

	t.Run("no maintenance mode orders", func(t *testing.T) {
		t.Parallel()

		orders := []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "a",
					Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
						CreateLedger: &raftcmdpb.CreateLedgerOrder{},
					},
				},
			}},
		}
		require.False(t, authorizedInMaintenanceMode(orders))
	})

	t.Run("empty orders", func(t *testing.T) {
		t.Parallel()
		require.True(t, authorizedInMaintenanceMode(nil))
		require.True(t, authorizedInMaintenanceMode([]*raftcmdpb.Order{}))
	})
}

func TestSealRequestFromChapter(t *testing.T) {
	t.Parallel()

	chapter := &commonpb.Chapter{
		Id:            5,
		CloseSequence: 42,
		LastAuditHash: []byte("hash-abc"),
	}

	req := SealRequestFromChapter(chapter)
	require.Equal(t, uint64(5), req.ChapterID)
	require.Equal(t, uint64(42), req.CloseSequence)
	require.Equal(t, []byte("hash-abc"), req.LastAuditHash)
}

func TestMachineAllChapters(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Initially empty
	require.Empty(t, machine.AllChapters())

	// Add chapters to machine
	machine.Chapters.PutChapter(&commonpb.Chapter{Id: 1, Status: commonpb.ChapterStatus_CHAPTER_CLOSED})
	machine.Chapters.PutChapter(&commonpb.Chapter{Id: 2, Status: commonpb.ChapterStatus_CHAPTER_OPEN})

	chapters := machine.AllChapters()
	require.Len(t, chapters, 2)
}

func TestMachineClosingChapters(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Initially empty
	require.Empty(t, machine.ClosingChapters())

	// Add closing chapter
	machine.Chapters.AddClosingChapter(&commonpb.Chapter{Id: 3, Status: commonpb.ChapterStatus_CHAPTER_CLOSING})
	require.Len(t, machine.ClosingChapters(), 1)
	cp, ok := machine.ClosingChapterByID(3)
	require.True(t, ok)
	require.Equal(t, uint64(3), cp.GetId())

	// Add a second closing chapter
	machine.Chapters.AddClosingChapter(&commonpb.Chapter{Id: 4, Status: commonpb.ChapterStatus_CHAPTER_CLOSING})
	require.Len(t, machine.ClosingChapters(), 2)

	// Remove first
	machine.Chapters.RemoveClosingChapter(3)
	require.Len(t, machine.ClosingChapters(), 1)
	_, ok = machine.ClosingChapterByID(3)
	require.False(t, ok)
}

func TestMachineChapterSchedule(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Initially empty
	require.Empty(t, machine.ChapterSchedule())

	// Set schedule
	machine.Chapters.SetSchedule("*/5 * * * *")
	require.Equal(t, "*/5 * * * *", machine.ChapterSchedule())
}

func TestMachineScheduleChanged(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	sig := machine.ScheduleChanged()
	require.NotNil(t, sig)
}

func TestMachineSealRequestAndArchiveRequestChannels(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// SealRequestCh and ArchiveRequestCh should return the channels
	sealCh := machine.SealRequestCh()
	require.NotNil(t, sealCh)

	archiveCh := machine.ArchiveRequestCh()
	require.NotNil(t, archiveCh)

	coldCh := machine.ColdCompactionCh()
	require.NotNil(t, coldCh)
}

func TestMachineLastPersistedIndex(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Initially 0
	idx := machine.LastPersistedIndex()
	require.Equal(t, uint64(0), idx)

	// Set via atomic
	machine.lastPersistedIndex.Store(42)
	idx = machine.LastPersistedIndex()
	require.Equal(t, uint64(42), idx)
}

func TestMachineIsStoreUpToDate(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	machine, dataStore, _ := newTestMachine(t)

	recovery := NewRecovery(machine, dataStore)
	sync := NewSynchronizer(machine, recovery, dal.NewIncomingRestoreFactory(dataStore))

	// With no snapshot and lastAppliedIndex=0, the store is up to date
	// (snapshotIndex == 0, lastAppliedIndex == 0)
	upToDate, err := sync.IsStoreUpToDate(ctx)
	require.NoError(t, err)
	require.True(t, upToDate)

	// If snapshot index is ahead of lastAppliedIndex, store is not up to date
	machine.State.SnapshotIndex = 10
	upToDate, err = sync.IsStoreUpToDate(ctx)
	require.NoError(t, err)
	require.False(t, upToDate)

	// Catch up lastAppliedIndex
	machine.State.LastAppliedIndex = 10
	upToDate, err = sync.IsStoreUpToDate(ctx)
	require.NoError(t, err)
	require.True(t, upToDate)
}

func TestReadLastLog(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Empty store
	log, err := query.ReadLastLog(handle)
	require.NoError(t, err)
	require.Nil(t, log)

	// Add logs
	registerLedger(t, s, "test-ledger")

	testLogs := createTestLogs("test-ledger")
	appendLogs(t, s, 1, testLogs...)

	log, err = query.ReadLastLog(handle)
	require.NoError(t, err)
	require.NotNil(t, log)
	require.Equal(t, uint64(4), log.GetSequence())
}

func TestReadAuditEntriesCursor(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	// Empty store
	cursor, err := query.ReadAuditEntries(context.Background(), handle, nil)
	require.NoError(t, err)

	_, curErr := cursor.Next()
	require.Error(t, curErr) // io.EOF

	_ = cursor.Close()

	// Add entries
	batch := s.OpenWriteSession()
	require.NoError(t, appendAuditEntries(batch,
		&auditpb.AuditEntry{Sequence: 1, ProposalId: 10, Timestamp: uint64(commonpb.NewTimestamp(libtime.Now()))},
		&auditpb.AuditEntry{Sequence: 2, ProposalId: 20, Timestamp: uint64(commonpb.NewTimestamp(libtime.Now()))},
		&auditpb.AuditEntry{Sequence: 3, ProposalId: 30, Timestamp: uint64(commonpb.NewTimestamp(libtime.Now()))},
	))
	require.NoError(t, batch.Commit())

	// Read all
	cursor, err = query.ReadAuditEntries(context.Background(), handle, nil)
	require.NoError(t, err)

	var entries []*auditpb.AuditEntry

	for {
		entry, nextErr := cursor.Next()
		if nextErr != nil {
			break
		}

		entries = append(entries, entry)
	}

	_ = cursor.Close()

	require.Len(t, entries, 3)

	// Read after sequence 1
	afterSeq := uint64(1)
	cursor, err = query.ReadAuditEntries(context.Background(), handle, &afterSeq)
	require.NoError(t, err)

	entries = nil

	for {
		entry, nextErr := cursor.Next()
		if nextErr != nil {
			break
		}

		entries = append(entries, entry)
	}

	_ = cursor.Close()

	require.Len(t, entries, 2)
	require.Equal(t, uint64(2), entries[0].GetSequence())
	require.Equal(t, uint64(3), entries[1].GetSequence())
}

func TestCheckCloseChapter(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// nil result
	require.Nil(t, machine.checkCloseChapter(nil))

	// result without CloseChapter log
	result := &ApplyResult{
		Logs: []*raftcmdpb.CreatedLogOrReference{
			{Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
				CreatedLog: &commonpb.Log{
					Sequence: 1,
					Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{LedgerName: "test-ledger"},
					}},
				},
			}},
		},
	}
	require.Nil(t, machine.checkCloseChapter(result))

	// result with CloseChapter log
	closedChapter := &commonpb.Chapter{
		Id:            5,
		CloseSequence: 42,
		LastAuditHash: []byte("hash"),
	}
	// Add the closing chapter on the machine so checkCloseChapter can find it.
	machine.Chapters.AddClosingChapter(closedChapter)
	resultWithClose := &ApplyResult{
		Logs: []*raftcmdpb.CreatedLogOrReference{
			{Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
				CreatedLog: &commonpb.Log{
					Sequence: 10,
					Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CloseChapter{
						CloseChapter: &commonpb.ClosedChapterLog{
							ClosedChapter: closedChapter,
						},
					}},
				},
			}},
		},
	}
	sealReq := machine.checkCloseChapter(resultWithClose)
	require.NotNil(t, sealReq)
	require.Equal(t, uint64(5), sealReq.ChapterID)
	require.Equal(t, uint64(42), sealReq.CloseSequence)
	require.Equal(t, []byte("hash"), sealReq.LastAuditHash)
}

func TestCheckCloseChapterReturnsLatestWhenMultiple(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)

	// Add two closing chapters — checkCloseChapter should return the latest
	firstChapter := &commonpb.Chapter{
		Id:            3,
		CloseSequence: 30,
		LastAuditHash: []byte("old-hash"),
	}
	latestChapter := &commonpb.Chapter{
		Id:            7,
		CloseSequence: 70,
		LastAuditHash: []byte("new-hash"),
	}
	machine.Chapters.AddClosingChapter(firstChapter)
	machine.Chapters.AddClosingChapter(latestChapter)

	resultWithClose := &ApplyResult{
		Logs: []*raftcmdpb.CreatedLogOrReference{
			{Type: &raftcmdpb.CreatedLogOrReference_CreatedLog{
				CreatedLog: &commonpb.Log{
					Sequence: 10,
					Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CloseChapter{
						CloseChapter: &commonpb.ClosedChapterLog{
							ClosedChapter: latestChapter,
						},
					}},
				},
			}},
		},
	}

	sealReq := machine.checkCloseChapter(resultWithClose)
	require.NotNil(t, sealReq)
	// Should return the latest closing chapter (chapter 7), not the first one
	require.Equal(t, uint64(7), sealReq.ChapterID)
	require.Equal(t, uint64(70), sealReq.CloseSequence)
	require.Equal(t, []byte("new-hash"), sealReq.LastAuditHash)
}
