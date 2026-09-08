package indexbuilder

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func seedCachedLedgerHistory(b *Builder, ledger string, state ledgerHistoryState) {
	if b.ledgerHistory == nil {
		b.ledgerHistory = make(map[string]ledgerHistoryState)
	}
	b.ledgerHistory[ledger] = state
}

func persistLedgerHistory(t *testing.T, b *Builder, ledger string, state ledgerHistoryState) {
	t.Helper()

	wb := readstore.NewWriteBatch()
	batch := b.readStore.NewBatch()
	wb.Init(batch)
	require.NoError(t, wb.WriteLedgerHistoryState(dal.NewKeyBuilder(), ledger, byte(state)))
	require.NoError(t, wb.Flush())
	seedCachedLedgerHistory(b, ledger, state)
}

// TestEmptyLedgerLateIndexSkipsGlobalBackfill is the EN-1771 regression.
// Ledger creation and index creation deliberately land in different proposals,
// with only CONTROL ledger logs between them. A correct indexbuilder remembers
// that no business HISTORY exists and never opens a global backfill cursor.
func TestEmptyLedgerLateIndexSkipsGlobalBackfill(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   *commonpb.IndexID
	}{
		{"generic metadata", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")},
		{"posting derived", indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			assertEmptyLedgerLateIndexSkipsGlobalBackfill(t, test.id)
		})
	}
}

func assertEmptyLedgerLateIndexSkipsGlobalBackfill(t *testing.T, id *commonpb.IndexID) {
	t.Helper()

	b := newTestBuilderWithStore(t)
	b.batchSize = DefaultBatchSize
	b.notifications = signal.NewNotifications()

	const ledger = "control-only"
	writeLogToFSM(t, b, &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
		}},
	})
	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), cursor)

	// The global log is mostly unrelated HISTORY for another ledger. A wrong
	// backfill decision would traverse all of it even though the target ledger
	// remains empty. Keep one target CONTROL entry as the cross-proposal guard.
	writeLogToFSM(t, b, &commonpb.Log{
		Sequence: 2,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: "busy-foreign-ledger"},
		}},
	})
	for seq := uint64(3); seq <= 32; seq++ {
		writeLogToFSM(t, b, &commonpb.Log{
			Sequence: seq,
			Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: "busy-foreign-ledger",
					Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_SavedMetadata{
							SavedMetadata: &commonpb.SavedMetadata{},
						},
					}).WithID(seq),
				},
			}},
		})
	}
	writeLogToFSM(t, b, &commonpb.Log{
		Sequence: 33,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
				Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_AddedAccountType{
						AddedAccountType: &commonpb.AddedAccountTypeLog{},
					},
				}).WithID(1),
			},
		}},
	})
	cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(33), cursor)

	writeLogToFSM(t, b, &commonpb.Log{
		Sequence: 34,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
				Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_CreateIndex{
						CreateIndex: &commonpb.CreatedIndexLog{Id: id},
					},
				}).WithID(34),
			},
		}},
	})

	cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(34), cursor)

	// Keep the pre-fix path observable: if it wrongly scheduled a task, execute
	// one complete pass and count the global cursor reads. The desired path has
	// no task, therefore performs exactly zero reads.
	var globalEntriesRead uint64
	if len(b.backfillTasks) > 0 {
		if isPostingIndex(id) {
			require.NoError(t, b.processBackfillPostings(
				context.Background(), make(chan struct{}), b.backfillTasks[0], time.Now().Add(time.Hour)))
		} else {
			require.NoError(t, b.processBackfill(
				context.Background(), make(chan struct{}), b.backfillTasks[0], time.Now().Add(time.Hour)))
		}
		// The fixture has dense global sequences starting at one, so the
		// persisted task cursor is an exact count of global entries consumed.
		globalEntriesRead = b.backfillTasks[0].cursor
	}

	assert.Zero(t, globalEntriesRead, "an empty ledger must cause zero global backfill reads")
	assert.Empty(t, b.backfillTasks, "an empty ledger's late index must be live immediately")
}

func TestSeveralIndexesOnFreshLedgersSkipLargeForeignHistory(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.batchSize = DefaultBatchSize
	b.notifications = signal.NewNotifications()

	writeLogToFSM(t, b, &commonpb.Log{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
		CreateLedger: &commonpb.CreatedLedgerLog{Name: "old-busy"},
	}}})
	for seq := uint64(2); seq <= 101; seq++ {
		writeLogToFSM(t, b, ledgerPayloadLog(seq, "old-busy", seq, &commonpb.LedgerLogPayload_OrderSkipped{
			OrderSkipped: &commonpb.OrderSkippedLog{},
		}, seq*10))
	}
	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(101), cursor)

	for seq, ledger := range map[uint64]string{102: "fresh-a", 103: "fresh-b"} {
		writeLogToFSM(t, b, &commonpb.Log{Sequence: seq, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
		}}})
	}
	cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(103), cursor)

	indexesByLedger := map[string][]*commonpb.IndexID{
		"fresh-a": {
			indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"),
			indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
		},
		"fresh-b": {
			indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
			indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET),
		},
	}
	sequence := uint64(104)
	for _, ledger := range []string{"fresh-a", "fresh-b"} {
		for _, id := range indexesByLedger[ledger] {
			writeLogToFSM(t, b, ledgerPayloadLog(sequence, ledger, sequence-103, &commonpb.LedgerLogPayload_CreateIndex{
				CreateIndex: &commonpb.CreatedIndexLog{Id: id},
			}, sequence*10))
			sequence++
		}
	}
	cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(107), cursor)

	assert.Empty(t, b.backfillTasks)
	for ledger, ids := range indexesByLedger {
		for _, id := range ids {
			current, pending := b.versionFor(ledger, indexes.Canonical(id))
			assert.NotZero(t, current)
			assert.Zero(t, pending)
			_, cursorExists := b.readStore.ReadBackfillProgress(backfillBBKey(ledger, id))
			assert.False(t, cursorExists)
		}
	}
}

func TestLedgerHistoryTrackerCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload *commonpb.LedgerLogPayload
		want    ledgerHistoryState
	}{
		{"created transaction", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{}}, ledgerHistoryNonEmpty},
		{"reverted transaction", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{}}, ledgerHistoryNonEmpty},
		{"saved metadata", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SavedMetadata{}}, ledgerHistoryNonEmpty},
		{"deleted metadata", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DeletedMetadata{}}, ledgerHistoryNonEmpty},
		{"order skipped", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_OrderSkipped{}}, ledgerHistoryNonEmpty},
		{"set metadata type", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{}}, ledgerHistoryEmpty},
		{"remove metadata type", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{}}, ledgerHistoryEmpty},
		{"fill gap", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_FillGap{}}, ledgerHistoryEmpty},
		{"create index", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreateIndex{}}, ledgerHistoryEmpty},
		{"drop index", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_DropIndex{}}, ledgerHistoryEmpty},
		{"add account type", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AddedAccountType{}}, ledgerHistoryEmpty},
		{"remove account type", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedAccountType{}}, ledgerHistoryEmpty},
		{"update default enforcement", &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{}}, ledgerHistoryEmpty},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBuilderWithStore(t)
			seedCachedLedgerHistory(b, "ledger", ledgerHistoryEmpty)
			batch := b.readStore.NewBatch()
			b.initFoldBatch(batch)
			require.NoError(t, b.observeLedgerPayload("ledger", test.payload))

			if b.wb.Empty() {
				require.NoError(t, batch.Cancel())
				b.rollbackFoldBatch()
			} else {
				require.NoError(t, b.wb.Flush())
				b.commitFoldBatch()
			}

			state, ok := b.historyStateFor("ledger")
			require.True(t, ok)
			assert.Equal(t, test.want, state)
		})
	}
}

func TestControlVersusHistoryAcrossProposalsDrivesCreateIndex(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		payload     any
		wantHistory ledgerHistoryState
		wantTask    bool
	}{
		{"FilledGap stays EMPTY", &commonpb.LedgerLogPayload_FillGap{FillGap: &commonpb.FilledGapLog{}}, ledgerHistoryEmpty, false},
		{"OrderSkipped becomes NON_EMPTY", &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{}}, ledgerHistoryNonEmpty, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBuilderWithStore(t)
			b.batchSize = DefaultBatchSize
			b.notifications = signal.NewNotifications()
			ledger := "classification"
			id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
			writeLogToFSM(t, b, &commonpb.Log{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
			}}})
			cursor, err := b.processLogs(context.Background(), 0, time.Time{})
			require.NoError(t, err)
			writeLogToFSM(t, b, ledgerPayloadLog(2, ledger, 1, test.payload, 20))
			cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
			require.NoError(t, err)
			writeLogToFSM(t, b, ledgerPayloadLog(3, ledger, 2, &commonpb.LedgerLogPayload_CreateIndex{
				CreateIndex: &commonpb.CreatedIndexLog{Id: id},
			}, 30))
			cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
			require.NoError(t, err)
			assert.Equal(t, uint64(3), cursor)
			state, exists := b.historyStateFor(ledger)
			require.True(t, exists)
			assert.Equal(t, test.wantHistory, state)
			if test.wantTask {
				assert.Len(t, b.backfillTasks, 1)
				current, pending := b.versionFor(ledger, indexes.Canonical(id))
				assert.Zero(t, current)
				assert.NotZero(t, pending)
			} else {
				assert.Empty(t, b.backfillTasks)
				current, pending := b.versionFor(ledger, indexes.Canonical(id))
				assert.NotZero(t, current)
				assert.Zero(t, pending)
			}
		})
	}
}

func TestHandleCreatedIndexLogHistoryStateMatrix(t *testing.T) {
	t.Parallel()

	indexTypes := []struct {
		name string
		id   *commonpb.IndexID
	}{
		{"transaction builtin", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)},
		{"metadata", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")},
		{"posting", indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)},
		{"log date", indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)},
	}

	for _, indexType := range indexTypes {
		for _, state := range []ledgerHistoryState{ledgerHistoryEmpty, ledgerHistoryNonEmpty} {
			name := indexType.name + "/" + map[ledgerHistoryState]string{
				ledgerHistoryEmpty: "empty", ledgerHistoryNonEmpty: "non-empty",
			}[state]
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				b := newTestBuilderWithStore(t)
				seedCachedLedgerHistory(b, "ledger", state)
				batch := b.readStore.NewBatch()
				b.initFoldBatch(batch)
				require.NoError(t, b.handleCreatedIndexLog("ledger", &commonpb.CreatedIndexLog{Id: indexType.id}))
				require.NoError(t, b.wb.Flush())
				b.commitFoldBatch()

				current, pending := b.versionFor("ledger", indexes.Canonical(indexType.id))
				if state == ledgerHistoryEmpty {
					assert.NotZero(t, current)
					assert.Zero(t, pending)
					assert.Empty(t, b.backfillTasks)
					_, cursorExists := b.readStore.ReadBackfillProgress(backfillBBKey("ledger", indexType.id))
					assert.False(t, cursorExists)
				} else {
					assert.Zero(t, current)
					assert.NotZero(t, pending)
					assert.Len(t, b.backfillTasks, 1)
				}
			})
		}
	}
}

func TestHandleCreatedIndexLogMissingHistoryFailsLoudly(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	batch := b.readStore.NewBatch()
	b.initFoldBatch(batch)
	err := b.handleCreatedIndexLog("missing", &commonpb.CreatedIndexLog{
		Id: indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
	})
	require.ErrorContains(t, err, "no EMPTY/NON_EMPTY history state")
	require.NoError(t, batch.Cancel())
	b.rollbackFoldBatch()
}

func TestLedgerHistoryRestartKeepsEmptyIndexLive(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	const ledger = "restart-empty"
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	canonical := indexes.Canonical(id)
	persistLedgerAndIndexRegistry(t, b, ledger, id)
	persistLedgerHistory(t, b, ledger, ledgerHistoryEmpty)

	batch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteIndexVersionState(batch, ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		HighWater:      1,
	}))
	require.NoError(t, b.readStore.WriteProgress(batch, 7))
	require.NoError(t, batch.Commit())

	cursor, _, err := b.bootInit(context.Background())
	require.NoError(t, err)
	assert.Equal(t, uint64(7), cursor)
	assert.Empty(t, b.backfillTasks)
	current, pending := b.versionFor(ledger, canonical)
	assert.Equal(t, uint32(1), current)
	assert.Zero(t, pending)
}

func TestFreshReadstoreReconstructsHistoryAndResolvesIndex(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.batchSize = DefaultBatchSize
	b.notifications = signal.NewNotifications()
	const ledger = "restored"
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	persistLedgerAndIndexRegistry(t, b, ledger, id)

	writeLogToFSM(t, b, &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
		}},
	})
	writeLogToFSM(t, b, ledgerPayloadLog(2, ledger, 1, &commonpb.LedgerLogPayload_AddedAccountType{
		AddedAccountType: &commonpb.AddedAccountTypeLog{},
	}, 20))
	writeLogToFSM(t, b, ledgerPayloadLog(3, ledger, 2, &commonpb.LedgerLogPayload_CreateIndex{
		CreateIndex: &commonpb.CreatedIndexLog{Id: id},
	}, 30))

	cursor, _, err := b.bootInit(context.Background())
	require.NoError(t, err)
	assert.Zero(t, cursor)
	require.Len(t, b.unresolvedIndexes[ledger], 1)

	cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, uint64(3), cursor)
	require.NoError(t, b.validateHistoryReplayState())
	assert.Empty(t, b.backfillTasks)
	current, pending := b.versionFor(ledger, indexes.Canonical(id))
	assert.NotZero(t, current)
	assert.Zero(t, pending)
	state, ok := b.historyStateFor(ledger)
	require.True(t, ok)
	assert.Equal(t, ledgerHistoryEmpty, state)
}

func TestFreshReadstoreFailsIfRegistryCreateWasNotReplayed(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	const ledger = "missing-create"
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	persistLedgerAndIndexRegistry(t, b, ledger, id)
	require.NoError(t, b.initIndexConfig(context.Background()))

	err := b.validateHistoryReplayState()
	require.ErrorContains(t, err, "was not resolved by CreatedIndex replay")
}

func TestLoadLedgerHistoryRejectsUnknownState(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	wb := readstore.NewWriteBatch()
	batch := b.readStore.NewBatch()
	wb.Init(batch)
	require.NoError(t, wb.WriteLedgerHistoryState(dal.NewKeyBuilder(), "corrupt", 99))
	require.NoError(t, wb.Flush())

	err := b.initIndexConfig(context.Background())
	require.ErrorContains(t, err, "unknown value 99")
}

func TestReplayValidationRejectsTrackerForInactiveLedger(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	persistLedgerHistory(t, b, "ghost", ledgerHistoryEmpty)
	require.NoError(t, b.initIndexConfig(context.Background()))

	err := b.validateHistoryReplayState()
	require.ErrorContains(t, err, "inactive ledger \"ghost\"")
}

func TestDeleteRecreateResetsLedgerHistoryIncarnation(t *testing.T) {
	t.Parallel()

	for _, separate := range []bool{false, true} {
		name := "same fold"
		if separate {
			name = "restart boundary"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			b := newTestBuilderWithStore(t)
			b.batchSize = DefaultBatchSize
			b.notifications = signal.NewNotifications()
			const ledger = "reused-name"
			id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
			logs := []*commonpb.Log{
				{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger}}}},
				ledgerPayloadLog(2, ledger, 1, &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{}}, 20),
				ledgerPayloadLog(3, ledger, 2, &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{Id: id}}, 30),
				{Sequence: 4, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: ledger}}}},
				{Sequence: 5, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger}}}},
				ledgerPayloadLog(6, ledger, 1, &commonpb.LedgerLogPayload_AddedAccountType{AddedAccountType: &commonpb.AddedAccountTypeLog{}}, 60),
				ledgerPayloadLog(7, ledger, 2, &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{Id: id}}, 70),
			}

			limit := len(logs)
			if separate {
				limit = 4
			}
			for _, log := range logs[:limit] {
				writeLogToFSM(t, b, log)
			}
			cursor, err := b.processLogs(context.Background(), 0, time.Time{})
			require.NoError(t, err)
			if separate {
				assert.Equal(t, uint64(4), cursor)
				_, exists := b.historyStateFor(ledger)
				assert.False(t, exists)
				assert.Empty(t, b.backfillTasks)
				for _, log := range logs[4:] {
					writeLogToFSM(t, b, log)
				}
				cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
				require.NoError(t, err)
			}

			assert.Equal(t, uint64(7), cursor)
			state, exists := b.historyStateFor(ledger)
			require.True(t, exists)
			assert.Equal(t, ledgerHistoryEmpty, state)
			assert.Empty(t, b.backfillTasks)
			current, pending := b.versionFor(ledger, indexes.Canonical(id))
			assert.NotZero(t, current)
			assert.Zero(t, pending)
		})
	}
}

func TestCreateIndexTrackerAndVersionRollbackTogether(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.batchSize = DefaultBatchSize
	b.notifications = signal.NewNotifications()
	const ledger = "atomic-empty"
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	canonical := indexes.Canonical(id)
	writeLogToFSM(t, b, &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
		}},
	})
	writeLogToFSM(t, b, ledgerPayloadLog(2, ledger, 1, &commonpb.LedgerLogPayload_CreateIndex{
		CreateIndex: &commonpb.CreatedIndexLog{Id: id},
	}, 20))

	corruptKey := dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAppliedProposal).
		PutUint64(3).
		Build()
	corrupt := b.pebbleStore.OpenWriteSession()
	require.NoError(t, corrupt.SetBytes(corruptKey, []byte{0x80}))
	require.NoError(t, corrupt.Commit())

	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.ErrorContains(t, err, "applied proposal cursor failed")
	assert.Zero(t, cursor)
	_, exists := b.historyStateFor(ledger)
	assert.False(t, exists)
	_, exists, err = b.readStore.ReadIndexVersionState(ledger, canonical)
	require.NoError(t, err)
	assert.False(t, exists)
	persistedCursor, err := b.readStore.LastIndexedSequence()
	require.NoError(t, err)
	assert.Zero(t, persistedCursor)
	assert.NotContains(t, b.indexConfig, ledger)

	repair := b.pebbleStore.OpenWriteSession()
	require.NoError(t, repair.SetProto(corruptKey, &proposalpb.AppliedProposal{Sequence: 3}))
	require.NoError(t, repair.Commit())
	cursor, err = b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), cursor)
	state, exists := b.historyStateFor(ledger)
	require.True(t, exists)
	assert.Equal(t, ledgerHistoryEmpty, state)
	version := mustReadVersionState(t, b, ledger, canonical)
	assert.NotZero(t, version.CurrentVersion)
	assert.Zero(t, version.PendingVersion)
	assert.Empty(t, b.backfillTasks)
}

func TestCommitFailureRollsBackHistoryVersionTaskAndCursor(t *testing.T) {
	t.Parallel()

	readstoreDir := t.TempDir()
	writableStore, err := readstore.New(readstoreDir, noopLogger{}, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = writableStore.Close() })
	checkpointDir := filepath.Join(t.TempDir(), "readindex")
	require.NoError(t, writableStore.CreateCheckpoint(checkpointDir))
	readOnlyStore, err := readstore.OpenReadOnly(checkpointDir, noopLogger{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = readOnlyStore.Close() })
	b := &Builder{
		indexConfig: make(map[string]*ledgerIndexConfig),
		readStore:   readOnlyStore,
		kb:          dal.NewKeyBuilder(),
		wb:          readstore.NewWriteBatch(),
		logger:      noopLogger{},
	}
	const ledger = "commit-retry"
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	canonical := indexes.Canonical(id)
	attempts := 0
	stage := func() error {
		attempts++
		batch := b.readStore.NewBatch()
		b.initFoldBatch(batch)
		if err := b.observeCreatedLedger(ledger); err != nil {
			return err
		}
		if err := b.observeLedgerPayload(ledger, &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{}},
		}); err != nil {
			return err
		}
		if err := b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{Id: id}); err != nil {
			return err
		}
		if err := b.readStore.WriteProgress(batch, 9); err != nil {
			return err
		}
		if err := b.flushWriteBatch(); err != nil {
			b.rollbackFoldBatch()

			return err
		}
		b.commitFoldBatch()

		return nil
	}

	require.Error(t, stage(), "committing a read-only Pebble batch must fail")
	_, historyExists := b.historyStateFor(ledger)
	assert.False(t, historyExists)
	_, versionExists, err := readOnlyStore.ReadIndexVersionState(ledger, canonical)
	require.NoError(t, err)
	assert.False(t, versionExists)
	persistedCursor, err := readOnlyStore.LastIndexedSequence()
	require.NoError(t, err)
	assert.Zero(t, persistedCursor)
	assert.NotContains(t, b.indexConfig, ledger)
	assert.Empty(t, b.backfillTasks)

	b.readStore = writableStore
	require.NoError(t, stage())
	assert.Equal(t, 2, attempts)
	history, historyExists := b.historyStateFor(ledger)
	require.True(t, historyExists)
	assert.Equal(t, ledgerHistoryNonEmpty, history)
	version := mustReadVersionState(t, b, ledger, canonical)
	assert.Zero(t, version.CurrentVersion)
	assert.NotZero(t, version.PendingVersion)
	require.Len(t, b.backfillTasks, 1)
	persistedCursor, err = b.readStore.LastIndexedSequence()
	require.NoError(t, err)
	assert.Equal(t, uint64(9), persistedCursor)
}

func TestCreateIndexRejectsHighWaterOverflow(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	const ledger = "version-exhausted"
	id := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
	seedCachedLedgerHistory(b, ledger, ledgerHistoryEmpty)
	b.putVersionState(ledger, indexes.Canonical(id), readstore.IndexVersionState{HighWater: ^uint32(0)})
	batch := b.readStore.NewBatch()
	b.initFoldBatch(batch)
	err := b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{Id: id})
	require.ErrorContains(t, err, "high-water exhausted")
	require.NoError(t, batch.Cancel())
	b.wb.Reset()
	b.rollbackFoldBatch()
}

func TestRetypeRejectsHighWaterOverflow(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	const ledger = "retype-version-exhausted"
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
	b.putVersionState(ledger, indexes.Canonical(id), readstore.IndexVersionState{HighWater: ^uint32(0)})
	batch := b.readStore.NewBatch()
	b.initFoldBatch(batch)
	err := b.bumpPendingVersion(ledger, id, commonpb.MetadataType_METADATA_TYPE_STRING)
	require.ErrorContains(t, err, "high-water exhausted")
	require.NoError(t, batch.Cancel())
	b.wb.Reset()
	b.rollbackFoldBatch()
}

func TestLogDateBackfillIncludesControlAndHistory(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	const ledger = "dated"
	id := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
	seedCachedLedgerHistory(b, ledger, ledgerHistoryNonEmpty)
	batch := b.readStore.NewBatch()
	b.initFoldBatch(batch)
	require.NoError(t, b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{Id: id}))
	require.NoError(t, b.wb.Flush())
	b.commitFoldBatch()
	require.Len(t, b.backfillTasks, 1)

	writeLogToFSM(t, b, ledgerPayloadLog(1, ledger, 11, &commonpb.LedgerLogPayload_AddedAccountType{
		AddedAccountType: &commonpb.AddedAccountTypeLog{},
	}, 101))
	writeLogToFSM(t, b, ledgerPayloadLog(2, ledger, 12, &commonpb.LedgerLogPayload_OrderSkipped{
		OrderSkipped: &commonpb.OrderSkippedLog{},
	}, 202))
	writeLogToFSM(t, b, ledgerPayloadLog(3, "foreign", 13, &commonpb.LedgerLogPayload_OrderSkipped{
		OrderSkipped: &commonpb.OrderSkippedLog{},
	}, 303))

	require.NoError(t, b.processBackfill(context.Background(), make(chan struct{}), b.backfillTasks[0], time.Now().Add(time.Hour)))
	prefix := readstore.LedgerLogDateRangePrefix(dal.NewKeyBuilder(), ledger)
	iter, err := b.readStore.DB().NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: readstore.IncrementBytes(prefix)})
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	require.NoError(t, iter.Error())
	assert.Equal(t, 2, count, "log_date must cover target-ledger CONTROL and HISTORY, but not foreign logs")
}

func TestEmptyLogDateFastPathIncludesEarlierControlLogs(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.batchSize = DefaultBatchSize
	b.notifications = signal.NewNotifications()
	const ledger = "control-dates"
	id := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
	writeLogToFSM(t, b, &commonpb.Log{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
		CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
	}}})
	writeLogToFSM(t, b, ledgerPayloadLog(2, ledger, 1, &commonpb.LedgerLogPayload_AddedAccountType{
		AddedAccountType: &commonpb.AddedAccountTypeLog{},
	}, 100))
	writeLogToFSM(t, b, ledgerPayloadLog(3, ledger, 2, &commonpb.LedgerLogPayload_AddedAccountType{
		AddedAccountType: &commonpb.AddedAccountTypeLog{},
	}, 200))
	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(3), cursor)

	writeLogToFSM(t, b, ledgerPayloadLog(4, ledger, 3, &commonpb.LedgerLogPayload_CreateIndex{
		CreateIndex: &commonpb.CreatedIndexLog{Id: id},
	}, 300))
	cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(4), cursor)
	assert.Empty(t, b.backfillTasks)
	current, pending := b.versionFor(ledger, indexes.Canonical(id))
	assert.NotZero(t, current)
	assert.Zero(t, pending)

	prefix := readstore.LedgerLogDateRangePrefix(dal.NewKeyBuilder(), ledger)
	iter, err := b.readStore.DB().NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: readstore.IncrementBytes(prefix)})
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	require.NoError(t, iter.Error())
	assert.Equal(t, 3, count, "fast promotion must retain earlier CONTROL dates and index CreateIndex itself")
}

func TestSpeculativeLogDatesArePurgedOnFirstHistory(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.batchSize = DefaultBatchSize
	b.notifications = signal.NewNotifications()
	const ledger = "no-date-index"
	writeLogToFSM(t, b, &commonpb.Log{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
		CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger},
	}}})
	writeLogToFSM(t, b, ledgerPayloadLog(2, ledger, 1, &commonpb.LedgerLogPayload_AddedAccountType{
		AddedAccountType: &commonpb.AddedAccountTypeLog{},
	}, 100))
	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), cursor)
	prefix := readstore.LedgerLogDateRangePrefix(dal.NewKeyBuilder(), ledger)
	assert.Equal(t, 1, countReadstorePrefix(t, b, prefix))

	writeLogToFSM(t, b, ledgerPayloadLog(3, ledger, 2, &commonpb.LedgerLogPayload_OrderSkipped{
		OrderSkipped: &commonpb.OrderSkippedLog{},
	}, 200))
	cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(3), cursor)
	assert.Zero(t, countReadstorePrefix(t, b, prefix))
}

func TestHistoricalDeletePreservesCurrentGenerationBuildStateAcrossRestart(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name string
		id   *commonpb.IndexID
	}{
		{"generic", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")},
		{"posting", indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBuilderWithStore(t)
			const ledger = "recreated"
			persistLedgerAndIndexRegistry(t, b, ledger, test.id)
			persistLedgerHistory(t, b, ledger, ledgerHistoryNonEmpty)
			batch := b.readStore.NewBatch()
			b.initFoldBatch(batch)
			require.NoError(t, b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{Id: test.id}))
			require.NoError(t, b.wb.Flush())
			b.commitFoldBatch()
			require.Len(t, b.backfillTasks, 1)

			writeLogToFSM(t, b, ledgerPayloadLog(1, ledger, 1, &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{}}, 10))
			writeLogToFSM(t, b, &commonpb.Log{Sequence: 2, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: ledger}}}})
			writeLogToFSM(t, b, &commonpb.Log{Sequence: 3, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: ledger}}}})
			writeLogToFSM(t, b, ledgerPayloadLog(4, ledger, 1, &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{}}, 40))

			task := b.backfillTasks[0]
			if isPostingIndex(test.id) {
				require.NoError(t, b.processBackfillPostings(context.Background(), make(chan struct{}), task, time.Now().Add(time.Hour)))
			} else {
				require.NoError(t, b.processBackfill(context.Background(), make(chan struct{}), task, time.Now().Add(time.Hour)))
			}
			assert.Equal(t, uint64(4), task.cursor)
			before := mustReadVersionState(t, b, ledger, indexes.Canonical(test.id))
			assert.Zero(t, before.CurrentVersion)
			assert.NotZero(t, before.PendingVersion)

			require.NoError(t, b.initIndexConfig(context.Background()))
			require.Len(t, b.backfillTasks, 1)
			assert.Equal(t, uint64(4), b.backfillTasks[0].cursor)
			after := mustCachedVersionState(t, b, ledger, indexes.Canonical(test.id))
			assert.Equal(t, before, after)
		})
	}
}

func TestHistoricalDeletePurgeSupportsEveryTransactionBuiltin(t *testing.T) {
	t.Parallel()

	for _, builtin := range []commonpb.TransactionBuiltinIndex{
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT,
	} {
		t.Run(builtin.String(), func(t *testing.T) {
			t.Parallel()

			b := newTestBuilderWithStore(t)
			batch := b.readStore.NewBatch()
			b.initBatch(batch)
			require.NoError(t, b.purgeBackfillTaskGeneration(&backfillTask{ledger: "ledger", index: indexes.TxBuiltinID(builtin)}))
			require.NoError(t, batch.Cancel())
			b.wb.Reset()
		})
	}
}

func persistLedgerAndIndexRegistry(t *testing.T, b *Builder, ledger string, id *commonpb.IndexID) {
	t.Helper()

	batch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))
	_, err := b.attrs.Index.Set(batch, domain.IndexKey{LedgerName: ledger, Canonical: indexes.Canonical(id)}.Bytes(), &commonpb.Index{
		Ledger:                 ledger,
		Id:                     id,
		ForwardEncodingVersion: 1,
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

func countReadstorePrefix(t *testing.T, b *Builder, prefix []byte) int {
	t.Helper()

	iter, err := b.readStore.DB().NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: readstore.IncrementBytes(prefix)})
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}
	require.NoError(t, iter.Error())

	return count
}

func ledgerPayloadLog(sequence uint64, ledger string, ledgerLogID uint64, payload any, date uint64) *commonpb.Log {
	data := &commonpb.LedgerLogPayload{}
	switch payload := payload.(type) {
	case *commonpb.LedgerLogPayload_AddedAccountType:
		data.Payload = payload
	case *commonpb.LedgerLogPayload_CreateIndex:
		data.Payload = payload
	case *commonpb.LedgerLogPayload_FillGap:
		data.Payload = payload
	case *commonpb.LedgerLogPayload_OrderSkipped:
		data.Payload = payload
	default:
		panic("unsupported ledgerPayloadLog test payload")
	}

	return &commonpb.Log{
		Sequence: sequence,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
				Log: &commonpb.LedgerLog{
					Id:   ledgerLogID,
					Date: &commonpb.Timestamp{Data: date},
					Data: data,
				},
			},
		}},
	}
}
