package indexbuilder

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestLogDateBackfillIncludesConfigurationHistory(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.backfillBudget = time.Hour
	const ledger = "history"
	id := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
	canonical := indexes.Canonical(id)
	metadataID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "status")
	payloads := []*commonpb.LedgerLogPayload{
		{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{}}},
		{Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{}}},
		{Payload: &commonpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &commonpb.SavedMetadata{}}},
		{Payload: &commonpb.LedgerLogPayload_DeletedMetadata{DeletedMetadata: &commonpb.DeletedMetadata{}}},
		{Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{Key: "status", TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Type: commonpb.MetadataType_METADATA_TYPE_INT64}}},
		{Payload: &commonpb.LedgerLogPayload_RemovedMetadataFieldType{RemovedMetadataFieldType: &commonpb.RemovedMetadataFieldTypeLog{Key: "status", TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, DroppedIndex: metadataID}}},
		{Payload: &commonpb.LedgerLogPayload_FillGap{FillGap: &commonpb.FilledGapLog{}}},
		{Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{Id: metadataID}}},
		{Payload: &commonpb.LedgerLogPayload_DropIndex{DropIndex: &commonpb.DroppedIndexLog{Id: id}}},
		{Payload: &commonpb.LedgerLogPayload_AddedAccountType{AddedAccountType: &commonpb.AddedAccountTypeLog{}}},
		{Payload: &commonpb.LedgerLogPayload_RemovedAccountType{RemovedAccountType: &commonpb.RemovedAccountTypeLog{}}},
		{Payload: &commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{UpdatedDefaultEnforcementMode: &commonpb.UpdatedDefaultEnforcementModeLog{}}},
		{Payload: &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{}}},
		{Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{Id: id}}},
	}
	for i, payload := range payloads {
		seq := uint64(i + 1)
		writeLogToFSM(t, b, logDateFixture(seq, ledger, payload))
	}
	// The global replay must not populate another ledger using this task's index.
	globalCursor := uint64(len(payloads) + 1)
	writeLogToFSM(t, b, logDateFixture(globalCursor, "foreign", payloads[9]))

	for _, incarnation := range []uint32{1, 2} {
		b.initBatch(b.readStore.NewBatch())
		if incarnation == 2 {
			require.NoError(t, b.handleDroppedIndexLog(b.kb, ledger, &commonpb.DroppedIndexLog{Id: id}))
		}
		require.NoError(t, b.handleCreatedIndexLog(ledger, &commonpb.CreatedIndexLog{Id: id}))
		require.NoError(t, b.wb.Flush())
		require.Len(t, b.backfillTasks, 1)
		task := b.backfillTasks[0]

		require.NoError(t, b.processBackfill(context.Background(), make(chan struct{}), task, time.Now().Add(time.Hour)))
		require.Equal(t, globalCursor, task.cursor)
		current, pending := b.versionFor(ledger, canonical)
		require.Zero(t, current, "history replay must not promote or drop the active build")
		require.Equal(t, incarnation, pending)
		require.Len(t, b.backfillTasks, 1, "historical CreateIndex must not create another task")
		require.Empty(t, b.schemaRewriteTasks, "historical schema changes must not be replayed")
		require.Len(t, b.ledgerConfig(ledger).byCanonical, 1, "historical lifecycle logs must not change live config")

		for i, payload := range payloads {
			seq := uint64(i + 1)
			key := readstore.LedgerLogDateKey(dal.NewKeyBuilder(), ledger, 100+seq, seq)
			_, closer, err := b.readStore.DB().Get(key)
			require.NoError(t, err, "incarnation %d: missing date for log %d (%T)", incarnation, seq, payload.GetPayload())
			require.NoError(t, closer.Close())
		}
		require.Equal(t, len(payloads), countKeysWithPrefix(t, b.readStore, readstore.LedgerLogDateRangePrefix(dal.NewKeyBuilder(), ledger)))
		require.Zero(t, countKeysWithPrefix(t, b.readStore, readstore.LedgerLogDateRangePrefix(dal.NewKeyBuilder(), "foreign")))

		b.processBackfills(context.Background(), make(chan struct{}), globalCursor)
		require.Empty(t, b.backfillTasks)
		current, pending = b.versionFor(ledger, canonical)
		require.Equal(t, incarnation, current)
		require.Zero(t, pending)
	}
}

func TestInitialLogDateIndexBackfillsItsOwnCreationAndEarlierConfiguration(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = 100
	b.backfillBudget = time.Hour
	const ledger = "new-ledger"
	id := indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
	writeLogToFSM(t, b, logDateFixture(1, ledger, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_AddedAccountType{AddedAccountType: &commonpb.AddedAccountTypeLog{}},
	}))
	writeLogToFSM(t, b, logDateFixture(2, ledger, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{Id: id, Initial: true}},
	}))
	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(2), cursor)
	current, pending := b.versionFor(ledger, indexes.Canonical(id))
	require.Zero(t, current, "born-empty is not empty of configuration logs")
	require.Equal(t, uint32(1), pending)
	require.Len(t, b.backfillTasks, 1)
	require.NoError(t, b.processBackfill(context.Background(), make(chan struct{}), b.backfillTasks[0], time.Now().Add(time.Hour)))
	b.processBackfills(context.Background(), make(chan struct{}), cursor)
	require.Empty(t, b.backfillTasks)
	current, pending = b.versionFor(ledger, indexes.Canonical(id))
	require.Equal(t, uint32(1), current)
	require.Zero(t, pending)

	// After promotion, the same configuration-log kind is indexed by the live
	// fold. Historical and subsequent dates must share the same keyspace.
	writeLogToFSM(t, b, logDateFixture(3, ledger, &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_AddedAccountType{AddedAccountType: &commonpb.AddedAccountTypeLog{}},
	}))
	cursor, err = b.processLogs(context.Background(), cursor, time.Time{})
	require.NoError(t, err)
	require.Equal(t, uint64(3), cursor)
	require.Equal(t, 3, countKeysWithPrefix(t, b.readStore, readstore.LedgerLogDateRangePrefix(dal.NewKeyBuilder(), ledger)))
	for seq := uint64(1); seq <= cursor; seq++ {
		_, closer, err := b.readStore.DB().Get(readstore.LedgerLogDateKey(dal.NewKeyBuilder(), ledger, 100+seq, seq))
		require.NoError(t, err)
		require.NoError(t, closer.Close())
	}
}

func logDateFixture(seq uint64, ledger string, payload *commonpb.LedgerLogPayload) *commonpb.Log {
	return &commonpb.Log{Sequence: seq, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: ledger, Log: &commonpb.LedgerLog{Id: seq, Date: &commonpb.Timestamp{Data: 100 + seq}, Data: payload},
	}}}}
}
