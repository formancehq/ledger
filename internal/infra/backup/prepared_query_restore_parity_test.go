package backup

import (
	"testing"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

func readPreparedQueryForRestore(t *testing.T, store *dal.Store, ledger, name string) *commonpb.PreparedQuery {
	t.Helper()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()

	prepared, err := attributes.New().PreparedQuery.Get(handle, domain.PreparedQueryKey{
		LedgerName: ledger,
		Name:       name,
	}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, prepared)

	return prepared
}

// TestBackup_PreparedQueryTargetChangeRestoreParity proves that an audited
// target/filter update after a first checkpoint survives a second checkpoint.
// The source projection comes from live apply; the destination projection
// comes from the second complete checkpoint, then the checker verifies it
// independently against the restored audit history.
func TestBackup_PreparedQueryTargetChangeRestoreParity(t *testing.T) {
	t.Parallel()

	const (
		bucketID = "prepared-query-target"
		ledger   = "ledger"
		name     = "by-id"
	)

	ctx := t.Context()
	storage := newInMemoryBackupStorage()
	sourceStore := newBackupTestStore(t)
	seedBackupTestAuditKey(t, sourceStore)
	source := newScriptedNode(t, sourceStore)
	source.applyClusterPolicy(&commonpb.ClusterPolicy{
		Revision:                    1,
		QueryCheckpointLimit:        10,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
		MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
		MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
		MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
	})

	oldFilter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{
		Reverted: &commonpb.RevertedCondition{Value: false},
	}}
	source.apply(actions.CreateLedgerAction(ledger, nil))
	source.apply(actions.CreatePreparedQueryAction(name, ledger, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, oldFilter))
	source.apply(actions.CreatePreparedQueryAction("removed", ledger, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, oldFilter))
	require.NoError(t, sourceStore.Flush())

	full, err := RunBackup(ctx, logging.Testing(), sourceStore, storage, bucketID, "full")
	require.NoError(t, err)
	require.Positive(t, full.LastLogSequence)
	checkpointQuery := readPreparedQueryForRestore(t, sourceStore, ledger, name)
	require.Equal(t, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, checkpointQuery.GetTarget())
	require.True(t, oldFilter.EqualVT(checkpointQuery.GetFilter()))

	logFilter, err := commonpb.DecodeQueryFilterJSON([]byte(`{"$match":{"id":7}}`), commonpb.QueryTarget_QUERY_TARGET_LOGS)
	require.NoError(t, err)
	source.apply(actions.UpdatePreparedQueryAction(ledger, name, commonpb.QueryTarget_QUERY_TARGET_LOGS, logFilter))
	source.apply(actions.DeletePreparedQueryAction(ledger, "removed"))
	source.apply(actions.CreatePreparedQueryAction("temporary", ledger, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, oldFilter))
	source.apply(actions.UpdatePreparedQueryAction(ledger, "temporary", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		&commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{Reverted: &commonpb.RevertedCondition{Value: true}}}))
	source.apply(actions.DeletePreparedQueryAction(ledger, "temporary"))
	require.NoError(t, sourceStore.Flush())
	live := readPreparedQueryForRestore(t, sourceStore, ledger, name)
	require.Equal(t, commonpb.QueryTarget_QUERY_TARGET_LOGS, live.GetTarget())
	require.True(t, logFilter.EqualVT(live.GetFilter()))
	auditKeyCheckerClean(t, sourceStore)

	second, err := RunBackup(ctx, logging.Testing(), sourceStore, storage, bucketID, "second")
	require.NoError(t, err)
	require.Greater(t, second.LastLogSequence, full.LastLogSequence, "the target-changing update must be in the second checkpoint")
	require.Greater(t, second.LastAuditSequence, full.LastAuditSequence)
	manifest, err := ReadManifest(ctx, storage, ManifestKey(bucketID))
	require.NoError(t, err)
	require.NotNil(t, manifest.Checkpoint)
	require.NotEmpty(t, manifest.Checkpoint.Files)
	require.Empty(t, manifest.Exports)

	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	require.True(t, live.EqualVT(readPreparedQueryForRestore(t, restored, ledger, name)),
		"second checkpoint must match the live target and filter")
	for _, deleted := range []string{"removed", "temporary"} {
		sourceHandle, err := sourceStore.NewDirectReadHandle()
		require.NoError(t, err)
		liveDeleted, err := attributes.New().PreparedQuery.Get(sourceHandle, domain.PreparedQueryKey{LedgerName: ledger, Name: deleted}.Bytes())
		require.NoError(t, err)
		require.NoError(t, sourceHandle.Close())
		require.Nil(t, liveDeleted)
		restoredHandle, err := restored.NewDirectReadHandle()
		require.NoError(t, err)
		restoredDeleted, err := attributes.New().PreparedQuery.Get(restoredHandle, domain.PreparedQueryKey{LedgerName: ledger, Name: deleted}.Bytes())
		require.NoError(t, err)
		require.NoError(t, restoredHandle.Close())
		require.Nil(t, restoredDeleted, "deleted query %q must not survive restore", deleted)
	}
	auditKeyCheckerClean(t, restored)

	// The next admission must consume the restored definition, including its
	// target, rather than a test-seeded projection.
	require.NoError(t, attributes.PrepareForBackup(restored))
	newScriptedNode(t, restored).apply(actions.UpdatePreparedQueryAction(
		ledger, name, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, oldFilter))
	require.Equal(t, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		readPreparedQueryForRestore(t, restored, ledger, name).GetTarget())
	auditKeyCheckerClean(t, restored)
}
