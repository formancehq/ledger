package backup

import (
	"testing"

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
// target/filter update after a real checkpoint survives incremental restore.
// The source projection comes from live apply; the destination projection
// comes from the checkpoint and exported delta, then the checker verifies it
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
	require.NoError(t, sourceStore.Flush())

	full, err := RunBackup(ctx, testLogger(), sourceStore, storage, bucketID, "full")
	require.NoError(t, err)
	require.Positive(t, full.LastLogSequence)
	checkpointQuery := readPreparedQueryForRestore(t, sourceStore, ledger, name)
	require.Equal(t, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, checkpointQuery.GetTarget())
	require.True(t, oldFilter.EqualVT(checkpointQuery.GetFilter()))

	logFilter, err := commonpb.DecodeQueryFilterJSON([]byte(`{"$match":{"id":7}}`), commonpb.QueryTarget_QUERY_TARGET_LOGS)
	require.NoError(t, err)
	source.apply(actions.UpdatePreparedQueryAction(ledger, name, commonpb.QueryTarget_QUERY_TARGET_LOGS, logFilter))
	require.NoError(t, sourceStore.Flush())
	live := readPreparedQueryForRestore(t, sourceStore, ledger, name)
	require.Equal(t, commonpb.QueryTarget_QUERY_TARGET_LOGS, live.GetTarget())
	require.True(t, logFilter.EqualVT(live.GetFilter()))
	auditKeyCheckerClean(t, sourceStore)

	inc, err := RunIncrementalBackup(ctx, testLogger(), sourceStore, storage, bucketID, 0)
	require.NoError(t, err)
	require.EqualValues(t, 1, inc.LogEntriesExported, "the target-changing update must be in the delta")
	require.Positive(t, inc.AuditEntriesExported)
	manifest, err := ReadManifest(ctx, storage, ManifestKey(bucketID))
	require.NoError(t, err)
	require.NotNil(t, manifest.Checkpoint)
	require.NotEmpty(t, manifest.Checkpoint.Files)
	require.NotEmpty(t, manifest.Exports)

	restored := restoreAuditKeyCheckpoint(t, storage, manifest)
	require.True(t, live.EqualVT(readPreparedQueryForRestore(t, restored, ledger, name)),
		"checkpoint plus delta must match the live target and filter")
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
