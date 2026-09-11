package node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const recoveryTestAppliedIndex = uint64(42)

// newRecoveryTestStore opens a main store whose live state is at
// recoveryTestAppliedIndex with the given checkpoints registered.
func newRecoveryTestStore(t *testing.T, checkpoints ...*raftcmdpb.QueryCheckpointState) *dal.Store {
	t.Helper()

	store, err := dal.NewStore(t.TempDir(), logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	batch := store.OpenWriteSession()
	require.NoError(t, state.SetAppliedIndex(batch, recoveryTestAppliedIndex))
	for _, cp := range checkpoints {
		require.NoError(t, state.SaveQueryCheckpoint(batch, cp))
	}
	require.NoError(t, batch.Commit())

	return store
}

func registeredCheckpoint(id, appliedIndex uint64) *raftcmdpb.QueryCheckpointState {
	return &raftcmdpb.QueryCheckpointState{CheckpointId: id, AppliedIndex: appliedIndex}
}

// requireFrozenMainStore asserts the checkpoint's main store is marked, has no
// temp sibling, and froze the live state: the applied index and the registry
// row itself.
func requireFrozenMainStore(t *testing.T, store *dal.Store, id uint64) {
	t.Helper()

	dir := store.QueryCheckpointMainDir(id)
	require.True(t, dal.CheckpointDirReady(dir), "main store must be marked ready")
	require.NoDirExists(t, dir+".tmp", "no temp directory may survive a materialization")

	frozen, err := dal.OpenReadOnly(dir, logging.Testing())
	require.NoError(t, err)
	defer func() { _ = frozen.Close() }()

	appliedIndex, err := query.ReadLastAppliedIndex(frozen)
	require.NoError(t, err)
	require.Equal(t, recoveryTestAppliedIndex, appliedIndex, "the frozen store must carry the checkpoint's applied index")

	row, err := query.ReadQueryCheckpoint(frozen, id)
	require.NoError(t, err)
	require.NotNil(t, row, "the frozen store must carry the checkpoint's own registry row")
}

// The residues below are produced from a finished materialization, so each is
// byte-for-byte what a process death at that step leaves behind: the copy
// itself is the same pebble checkpoint the production path writes.
func TestRecoverQueryCheckpointMainStoresRebuildsEveryCrashResidue(t *testing.T) {
	t.Parallel()

	residues := map[string]func(t *testing.T, mainDir string){
		"died before the temp checkpoint": func(t *testing.T, mainDir string) {
			require.NoError(t, os.RemoveAll(mainDir))
		},
		"died during the temp copy": func(t *testing.T, mainDir string) {
			tmpDir := mainDir + ".tmp"
			require.NoError(t, os.Rename(mainDir, tmpDir))
			require.NoError(t, os.Remove(filepath.Join(tmpDir, ".ready")))
			wals, err := filepath.Glob(filepath.Join(tmpDir, "*.log"))
			require.NoError(t, err)
			require.NotEmpty(t, wals, "the copy must have a WAL file to lose")
			for _, wal := range wals {
				require.NoError(t, os.Remove(wal))
			}
		},
		"died after the temp copy, before the marker": func(t *testing.T, mainDir string) {
			tmpDir := mainDir + ".tmp"
			require.NoError(t, os.Rename(mainDir, tmpDir))
			require.NoError(t, os.Remove(filepath.Join(tmpDir, ".ready")))
		},
		"died after the marker, before the rename": func(t *testing.T, mainDir string) {
			require.NoError(t, os.Rename(mainDir, mainDir+".tmp"))
		},
		"final directory without a marker": func(t *testing.T, mainDir string) {
			require.NoError(t, os.Remove(filepath.Join(mainDir, ".ready")))
		},
	}

	for name, leaveResidue := range residues {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			store := newRecoveryTestStore(t, registeredCheckpoint(1, recoveryTestAppliedIndex))
			_, err := store.CreateQueryCheckpoint(1)
			require.NoError(t, err)

			leaveResidue(t, store.QueryCheckpointMainDir(1))
			require.False(t, dal.CheckpointDirReady(store.QueryCheckpointMainDir(1)), "the residue must read as unmaterialized")

			require.NoError(t, recoverQueryCheckpointMainStores(store, logging.Testing(), recoveryTestAppliedIndex))
			requireFrozenMainStore(t, store, 1)
		})
	}
}

func TestRecoverQueryCheckpointMainStoresLeavesMarkedStoreUntouched(t *testing.T) {
	t.Parallel()

	store := newRecoveryTestStore(t, registeredCheckpoint(1, recoveryTestAppliedIndex))
	_, err := store.CreateQueryCheckpoint(1)
	require.NoError(t, err)

	sentinel := filepath.Join(store.QueryCheckpointMainDir(1), "sentinel")
	require.NoError(t, os.WriteFile(sentinel, nil, 0o600))

	require.NoError(t, recoverQueryCheckpointMainStores(store, logging.Testing(), recoveryTestAppliedIndex))

	require.FileExists(t, sentinel, "a marked main store must not be rebuilt")
	requireFrozenMainStore(t, store, 1)
}

func TestRecoverQueryCheckpointMainStoresSkipsCheckpointsNotAtTheLiveIndex(t *testing.T) {
	t.Parallel()

	store := newRecoveryTestStore(t,
		registeredCheckpoint(1, recoveryTestAppliedIndex-5),
		registeredCheckpoint(2, recoveryTestAppliedIndex),
		&raftcmdpb.QueryCheckpointState{CheckpointId: 3, AppliedIndex: recoveryTestAppliedIndex, RestoredFromBackup: true},
	)

	require.NoError(t, recoverQueryCheckpointMainStores(store, logging.Testing(), recoveryTestAppliedIndex))

	require.NoDirExists(t, store.QueryCheckpointMainDir(1), "a checkpoint applied at another index cannot be rebuilt from this live store")
	requireFrozenMainStore(t, store, 2)
	require.NoDirExists(t, store.QueryCheckpointMainDir(3), "a restored checkpoint's index belongs to the source cluster")
}

// RecoverAndReplay is the production caller: a checkpoint registered at the
// store's applied index with no main store is rebuilt during startup recovery.
func TestRecoverAndReplayRebuildsUnmaterializedQueryCheckpoint(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	liveAppliedIndex, err := query.ReadLastAppliedIndex(setup.store)
	require.NoError(t, err)

	batch := setup.store.OpenWriteSession()
	require.NoError(t, state.SaveQueryCheckpoint(batch, registeredCheckpoint(7, liveAppliedIndex)))
	require.NoError(t, batch.Commit())
	require.False(t, dal.CheckpointDirReady(setup.store.QueryCheckpointMainDir(7)))

	upToDate, err := setup.applier.RecoverAndReplay(logging.TestingContext())
	require.NoError(t, err)
	require.True(t, upToDate)

	require.True(t, dal.CheckpointDirReady(setup.store.QueryCheckpointMainDir(7)))
	require.NoDirExists(t, setup.store.QueryCheckpointMainDir(7)+".tmp")
}
