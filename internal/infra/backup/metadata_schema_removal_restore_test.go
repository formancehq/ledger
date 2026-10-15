package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// readLedgerInfo reads the LedgerInfo the query path serves for GetLedger.
func readLedgerInfo(t *testing.T, store *dal.Store, ledger string) *commonpb.LedgerInfo {
	t.Helper()

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, handle.Close()) }()

	info, err := query.GetLedgerByName(t.Context(), handle, ledger)
	require.NoError(t, err)

	return info
}

// TestBackup_RemoveUndeclaredFieldTypeOnAbsentSchemaRestoreParity is the
// cross-lifecycle proof (invariant #11) for a no-op metadata field type
// removal on a ledger that has no metadata schema. Live apply keeps
// MetadataSchema unset (GetLedger omits metadataSchema), so the restore
// rebuild from checkpoint(prefix) plus a delta holding only that removal must
// keep it unset too. The checker treats a nil and an empty schema as equal, so
// schema presence is compared directly between the live and restored
// LedgerInfo, independently of the checker.
func TestBackup_RemoveUndeclaredFieldTypeOnAbsentSchemaRestoreParity(t *testing.T) {
	t.Parallel()

	for _, target := range []commonpb.TargetType{
		commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		commonpb.TargetType_TARGET_TYPE_TRANSACTION,
		commonpb.TargetType_TARGET_TYPE_LEDGER,
	} {
		t.Run(target.String(), func(t *testing.T) {
			t.Parallel()

			const (
				bucketID = "bucket"
				ledger   = "no-schema"
				key      = "never-declared"
			)

			ctx := context.Background()
			storage := newInMemoryBackupStorage()

			srcStore := newBackupTestStore(t)
			seedBackupTestAuditKey(t, srcStore)
			src := newScriptedNode(t, srcStore)

			// The policy goes through an audited order so the checker can
			// account for the stored row.
			src.applyClusterPolicy(&commonpb.ClusterPolicy{
				Revision:                    1,
				QueryCheckpointLimit:        10,
				MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
				MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
				MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
				MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
				MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
			})

			// Before the checkpoint: a ledger created without an initial
			// schema, so the checkpoint seeds a LedgerInfo with no schema.
			src.apply(actions.CreateLedgerAction(ledger, nil))
			require.NoError(t, srcStore.Flush())
			require.Nil(t, readLedgerInfo(t, srcStore, ledger).GetMetadataSchema(), "premise: the ledger starts without a schema")

			full, err := RunBackup(ctx, testLogger(), srcStore, storage, bucketID, "bk-full")
			require.NoError(t, err)

			// After the checkpoint: remove the type of a key that was never
			// declared.
			src.apply(actions.RemoveMetadataFieldTypeAction(ledger, target, key))
			require.NoError(t, srcStore.Flush())

			live := readLedgerInfo(t, srcStore, ledger)
			require.Nil(t, live.GetMetadataSchema(), "live: a no-op removal must not materialize an empty schema")
			auditKeyCheckerClean(t, srcStore)

			inc, err := RunIncrementalBackup(ctx, testLogger(), srcStore, storage, bucketID, 0)
			require.NoError(t, err)
			require.EqualValues(t, 1, inc.LogEntriesExported, "the delta must hold exactly the removal log")
			require.Positive(t, inc.AuditEntriesExported)
			require.Greater(t, inc.LastLogSequence, full.LastLogSequence)

			// The single exported log is the removal, still logged although
			// the schema did not change.
			handle, err := srcStore.NewReadHandle()
			require.NoError(t, err)
			exported, err := query.ReadLogBySequence(ctx, handle, inc.LastLogSequence)
			require.NoError(t, handle.Close())
			require.NoError(t, err)
			removed := exported.GetPayload().GetApply().GetLog().GetData().GetRemovedMetadataFieldType()
			require.NotNil(t, removed, "the delta must contain the removal")
			require.Equal(t, target, removed.GetTargetType())
			require.Equal(t, key, removed.GetKey())
			require.Nil(t, removed.GetDroppedIndex())

			manifest, err := ReadManifest(ctx, storage, ManifestKey(bucketID))
			require.NoError(t, err)
			require.NotNil(t, manifest.Checkpoint)
			require.NotEmpty(t, manifest.Exports)

			// The restored cluster: the checkpoint files, then the delta.
			restored := restoreAuditKeyCheckpoint(t, storage, manifest)

			restoredInfo := readLedgerInfo(t, restored, ledger)
			require.Nil(t, restoredInfo.GetMetadataSchema(), "restored: schema presence must match live")
			require.True(t, live.EqualVT(restoredInfo), "restored LedgerInfo must match live")
			auditKeyCheckerClean(t, restored)

			// The next decision consumes the restored projection: declaring
			// the key on the restored store creates the schema with only
			// that field.
			require.NoError(t, attributes.PrepareForBackup(restored))
			newScriptedNode(t, restored).apply(actions.SetMetadataFieldTypeAction(ledger, target, key, commonpb.MetadataType_METADATA_TYPE_INT64))
			require.NoError(t, restored.Flush())

			after := readLedgerInfo(t, restored, ledger).GetMetadataSchema()
			require.NotNil(t, after)
			fields := map[commonpb.TargetType]map[string]*commonpb.MetadataFieldSchema{
				commonpb.TargetType_TARGET_TYPE_ACCOUNT:     after.GetAccountFields(),
				commonpb.TargetType_TARGET_TYPE_TRANSACTION: after.GetTransactionFields(),
				commonpb.TargetType_TARGET_TYPE_LEDGER:      after.GetLedgerFields(),
			}
			for candidate, declared := range fields {
				if candidate == target {
					require.Len(t, declared, 1)
					require.Equal(t, commonpb.MetadataType_METADATA_TYPE_INT64, declared[key].GetType())
				} else {
					require.Empty(t, declared)
				}
			}
			auditKeyCheckerClean(t, restored)
		})
	}
}
