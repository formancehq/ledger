package indexbuilder

import (
	"context"
	"testing"
	"time"

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

// TestRetypeDuringBackfill_FailedFoldRollsBackThenRetriesAndRestarts proves
// the builder-local reset follows the same commit boundary as its durable
// pending-version/cursor writes. The first real processLogs attempt reaches
// the retype and then fails on a corrupt AppliedProposal encountered by the
// progress step. The second attempt repairs that input and commits, after
// which initIndexConfig exercises the crash-recovery path on the same stores.
func TestRetypeDuringBackfill_FailedFoldRollsBackThenRetriesAndRestarts(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.notifications = signal.NewNotifications()
	b.batchSize = DefaultBatchSize

	const (
		ledger = "customer"
		key    = "role"
	)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key)
	canonical := indexes.Canonical(id)
	before := readstore.IndexVersionState{
		CurrentVersion:  0,
		PendingVersion:  1,
		HighWater:       1,
		RewriteProgress: []byte{},
	}

	stateBatch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteIndexVersionState(stateBatch, ledger, canonical, before))
	require.NoError(t, stateBatch.Commit())

	backfillKey := backfillBBKey(ledger, id)
	progressBatch := b.readStore.NewBatch()
	require.NoError(t, b.readStore.WriteBackfillProgress(progressBatch, backfillKey, 77))
	require.NoError(t, progressBatch.Commit())

	fsmBatch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.SaveLedger(fsmBatch, ledger, &commonpb.LedgerInfo{
		Name: ledger,
		MetadataSchema: &commonpb.MetadataSchema{
			TransactionFields: map[string]*commonpb.MetadataFieldSchema{
				key: {Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
			},
		},
	}))
	indexKey := domain.IndexKey{LedgerName: ledger, Canonical: canonical}.Bytes()
	_, err := b.attrs.Index.Set(fsmBatch, indexKey, &commonpb.Index{
		Ledger:                 ledger,
		Id:                     id,
		ForwardEncodingVersion: 2,
	})
	require.NoError(t, err)
	require.NoError(t, fsmBatch.Commit())

	writeLogToFSM(t, b, &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
				Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
					Payload: &commonpb.LedgerLogPayload_SetMetadataFieldType{
						SetMetadataFieldType: &commonpb.SetMetadataFieldTypeLog{
							TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
							Key:        key,
							Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
						},
					},
				}).WithID(1),
			},
		}},
	})
	writeAppliedProposalToFSM(t, b, 1, 1, 1)

	corruptProposalKey := dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAppliedProposal).
		PutUint64(2).
		Build()
	corruptBatch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, corruptBatch.SetBytes(corruptProposalKey, []byte{0x80}))
	require.NoError(t, corruptBatch.Commit())

	require.NoError(t, b.initIndexConfig(context.Background()))
	require.Len(t, b.backfillTasks, 1)
	require.Equal(t, uint64(77), b.backfillTasks[0].cursor)
	attempts := 0
	attempts++
	cursor, err := b.processLogs(context.Background(), 0, time.Time{})
	require.ErrorContains(t, err, "applied proposal cursor failed")
	assert.Zero(t, cursor)
	assert.Equal(t, before, mustReadVersionState(t, b, ledger, canonical),
		"failed fold must leave durable pending state unchanged")
	assert.Equal(t, before, mustCachedVersionState(t, b, ledger, canonical),
		"failed fold must roll back the in-memory pending bump")
	require.Len(t, b.backfillTasks, 1)
	assert.Equal(t, uint64(77), b.backfillTasks[0].cursor,
		"failed fold must roll back the in-memory backfill reset")
	assert.Equal(t, uint64(0), b.lastAppliedProposalSeq)
	persistedCursor, ok := b.readStore.ReadBackfillProgress(backfillKey)
	require.True(t, ok)
	assert.Equal(t, uint64(77), persistedCursor,
		"failed fold must retain the durable cursor paired with pending v1")

	repairBatch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, repairBatch.SetProto(corruptProposalKey, &proposalpb.AppliedProposal{Sequence: 2}))
	require.NoError(t, repairBatch.Commit())

	attempts++
	cursor, err = b.processLogs(context.Background(), 0, time.Time{})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), cursor)
	assert.Equal(t, 2, attempts, "regression must execute one failed attempt followed by one success")

	after := mustReadVersionState(t, b, ledger, canonical)
	assert.Equal(t, uint32(2), after.PendingVersion)
	assert.Equal(t, uint32(2), after.HighWater)
	assert.True(t, after.PendingTypeDeclared)
	assert.Equal(t, commonpb.MetadataType_METADATA_TYPE_UINT64, after.PendingType)
	_, ok = b.readStore.ReadBackfillProgress(backfillKey)
	assert.False(t, ok, "successful retry must commit the cursor reset")

	// Simulate a crash after the successful retry: discard every local cache
	// and reconstruct from the durable main/read stores.
	require.NoError(t, b.initIndexConfig(context.Background()))
	require.Len(t, b.backfillTasks, 1)
	assert.Zero(t, b.backfillTasks[0].cursor)
	assert.Empty(t, b.schemaRewriteTasks)
	assert.Equal(t, after, mustCachedVersionState(t, b, ledger, canonical))
}

func mustReadVersionState(t *testing.T, b *Builder, ledger, canonical string) readstore.IndexVersionState {
	t.Helper()

	state, ok, err := b.readStore.ReadIndexVersionState(ledger, canonical)
	require.NoError(t, err)
	require.True(t, ok)

	return state
}

func mustCachedVersionState(t *testing.T, b *Builder, ledger, canonical string) readstore.IndexVersionState {
	t.Helper()

	state, ok := b.versionStateFor(ledger, canonical)
	require.True(t, ok)

	return state
}
