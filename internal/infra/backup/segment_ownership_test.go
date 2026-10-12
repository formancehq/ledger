package backup

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// hash64 is a well-formed stand-in for the hex sha256 CheckpointFileKey appends.
func hash64() string { return strings.Repeat("ab", sha256HexLen/2) }

// TestKeyBuildersProduceOwnedKeys pins every production key builder to the
// recogniser that guards its prune. If a builder's shape changes and the
// recogniser is not updated with it, the prune silently stops deleting that
// chain's own orphans — a leak rather than a loud failure — so this is the
// anti-drift gate for that pairing.
func TestKeyBuildersProduceOwnedKeys(t *testing.T) {
	t.Parallel()

	const bucketID = "tenant"

	t.Run("checkpoint", func(t *testing.T) {
		t.Parallel()

		for _, filename := range []string{
			"000005.sst",
			"MANIFEST-000001",
			"OPTIONS-000003",
			"marker.format-version.000001.024",
		} {
			key := CheckpointFileKey(bucketID, filename, hash64())
			require.True(t, ownsCheckpointKey(bucketID, key),
				"CheckpointFileKey output must be recognised as owned: %s", key)
		}
	})

	t.Run("exports", func(t *testing.T) {
		t.Parallel()

		for name, key := range map[string]string{
			"log":             ExportLogSegmentKey(bucketID, 1, 2, 0),
			"audit":           ExportAuditSegmentKey(bucketID, 1, 2, 0),
			"auditItem":       ExportAuditItemSegmentKey(bucketID, 1, 2, 0),
			"appliedProposal": ExportAppliedProposalSegmentKey(bucketID, 1, 2, 0),
		} {
			require.True(t, ownsExportKey(bucketID, key),
				"%s segment key must be recognised as owned: %s", name, key)
		}
	})
}

// TestOwnsKeyRejectsForeignShapes covers the keys a prune must refuse to
// delete. The nested cases are the destructive ones: a second backup chain
// whose bucket-id sits under this chain's data/ or exports/ root is returned by
// this chain's listing, and deleting those objects destroys that backup.
func TestOwnsKeyRejectsForeignShapes(t *testing.T) {
	t.Parallel()

	const bucketID = "tenant"

	t.Run("checkpoint root", func(t *testing.T) {
		t.Parallel()

		for name, key := range map[string]string{
			"nested chain manifest":   "tenant/data/archive/manifest.json",
			"nested chain checkpoint": "tenant/data/archive/data/000005.sst." + hash64(),
			"nested chain export":     "tenant/data/archive/exports/logs-x.bin",
			"no content hash":         "tenant/data/000005.sst",
			"short hash":              "tenant/data/000005.sst.abc",
			"upper-case hash":         "tenant/data/000005.sst." + strings.ToUpper(hash64()),
			"another chain entirely":  "other/data/000005.sst." + hash64(),
		} {
			require.False(t, ownsCheckpointKey(bucketID, key),
				"%s must not be owned: %s", name, key)
		}
	})

	t.Run("export root", func(t *testing.T) {
		t.Parallel()

		for name, key := range map[string]string{
			"nested chain manifest":   "tenant/exports/archive/manifest.json",
			"nested chain checkpoint": "tenant/exports/archive/data/000005.sst." + hash64(),
			"nested chain export":     "tenant/exports/archive/exports/logs-x.bin",
			"wrong extension":         "tenant/exports/logs-x.seg",
			"another chain entirely":  "other/exports/logs-x.bin",
		} {
			require.False(t, ownsExportKey(bucketID, key),
				"%s must not be owned: %s", name, key)
		}
	})
}

// TestPruneOrphans_LeavesNestedChainObjects verifies at the prune unit that a
// nested chain's objects are listed but never deleted, while this chain's own
// orphan still is. Asserting the single DeleteFile call is what distinguishes
// "skipped the nested objects" from "deleted nothing at all".
func TestPruneOrphans_LeavesNestedChainObjects(t *testing.T) {
	t.Parallel()

	const bucketID = "tenant"

	ownOrphan := CheckpointFileKey(bucketID, "leaked.sst", hash64())
	nested := []string{
		"tenant/data/archive/manifest.json",
		"tenant/data/archive/data/000005.sst." + hash64(),
	}

	storage := NewMockStorage(gomock.NewController(t))
	storage.EXPECT().ListFiles(gomock.Any(), CheckpointPrefix(bucketID)).
		Return(append([]string{ownOrphan}, nested...), nil)
	storage.EXPECT().DeleteFile(gomock.Any(), ownOrphan).Return(nil)

	deleted := pruneOrphans(context.Background(), logging.Testing(), storage,
		checkpointKeyspace(bucketID), nil)

	require.Equal(t, []string{ownOrphan}, deleted,
		"only this chain's own orphan may be pruned")
}

// TestRunBackup_PreservesNestedChain is the end-to-end regression for the
// destructive case: a complete backup chain whose bucket-id nests under another
// chain's data/ or exports/ root must survive that other chain's full backup,
// manifest included.
//
// Before the ownership rule, the parent's prune deleted every object of the
// child chain — the listing by prefix returns them, and the export prune runs
// with no expected set at all.
func TestRunBackup_PreservesNestedChain(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		parent string
		child  string
	}{
		{"child under the parent's data root", "tenant", "tenant/data/archive"},
		{"child under the parent's export root", "tenant", "tenant/exports/archive"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newBackupTestStore(t)

			seed := store.OpenWriteSession()
			require.NoError(t, seed.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
			require.NoError(t, seed.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
			require.NoError(t, seed.Commit())
			require.NoError(t, store.Flush())

			storage := newInMemoryBackupStorage()

			// The child chain completes a full backup first.
			_, err := RunBackup(context.Background(), logging.Testing(), store, storage, tc.child, "child-1")
			require.NoError(t, err)

			childManifest, err := ReadManifest(context.Background(), storage, ManifestKey(tc.child))
			require.NoError(t, err)
			require.NotNil(t, childManifest.Checkpoint, "child chain must have a checkpoint")

			childKeys := []string{ManifestKey(tc.child)}
			for _, cf := range childManifest.Checkpoint.Files {
				childKeys = append(childKeys, cf.Key)
			}

			// Give the child an export segment too, so both of the parent's
			// prune roots are exercised against it.
			childExport := ExportLogSegmentKey(tc.child, 1, 2, 0)
			require.NoError(t, storage.PutFile(context.Background(), childExport,
				bytes.NewReader([]byte("segment")), int64(len("segment"))))
			childKeys = append(childKeys, childExport)

			require.Greater(t, len(childKeys), 2, "child chain must have real objects to lose")

			// The parent chain now runs its own full backup, which prunes both
			// of its roots.
			_, err = RunBackup(context.Background(), logging.Testing(), store, storage, tc.parent, "parent-1")
			require.NoError(t, err)

			for _, key := range childKeys {
				_, err := storage.GetFile(context.Background(), key)
				require.NoErrorf(t, err,
					"the parent's prune destroyed a live object of the nested chain %q: %s", tc.child, key)
			}

			// The child chain must still be readable as a chain, not merely
			// have its bytes present.
			restored, err := ReadManifest(context.Background(), storage, ManifestKey(tc.child))
			require.NoError(t, err, "the nested chain's manifest must survive")
			require.NotNil(t, restored.Checkpoint)
		})
	}
}
