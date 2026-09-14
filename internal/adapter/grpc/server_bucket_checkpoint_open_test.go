package grpc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestOpenCheckpointStoresReadIndexDeletion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name                              string
		deleted, corrupt, removeDirectory bool
	}{
		{name: "deleted checkpoint returns NotFound", deleted: true},
		{name: "live checkpoint missing database remains an error"},
		{name: "deleted checkpoint directory disappears during main open", deleted: true, removeDirectory: true},
		{name: "live checkpoint directory disappearance remains an error", removeDirectory: true},
		{name: "deleted checkpoint corruption remains an error", deleted: true, corrupt: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			impl, _ := newCheckpointWaitHarness(t)
			const id = uint64(1)
			// Test-only fixtures emulate the committed lifecycle batches, and create
			// real database checkpoints before simulating file removal.
			batch := impl.store.OpenWriteSession()
			require.NoError(t, state.SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: id}))
			require.NoError(t, state.StoreNextQueryCheckpointID(batch, id+1))
			require.NoError(t, batch.Commit())
			_, err := impl.store.CreateQueryCheckpoint(id)
			require.NoError(t, err)
			indexPath := impl.store.QueryCheckpointReadIndexDir(id)
			require.NoError(t, impl.readStore.CreateCheckpoint(indexPath))
			require.NoError(t, readstore.MarkCheckpointReady(indexPath))
			main, index, err := impl.openCheckpointStores(context.Background(), id)
			require.NoError(t, err, "the checkpoint must be readable before deletion")
			require.NoError(t, index.Close())
			require.NoError(t, main.Close())
			if tc.deleted {
				batch = impl.store.OpenWriteSession()
				require.NoError(t, state.DeleteQueryCheckpointFromBatch(batch, id))
				require.NoError(t, batch.Commit())
			}
			// Pause recursive deletion after database files disappear but before the
			// readiness marker does. This deterministically reaches the second-open
			// race through the real adapter, without sleeps or a production test hook.
			entries, err := os.ReadDir(indexPath)
			require.NoError(t, err)
			manifests := 0
			if tc.removeDirectory {
				// Pebble logs synchronously while opening the main DB. Use that real
				// event to finish removing the read index after the marker check.
				impl.logger = &checkpointDirectoryDeletingLogger{remove: func() {
					require.NoError(t, os.RemoveAll(indexPath))
				}}
			}
			for _, entry := range entries {
				if tc.removeDirectory {
					break
				}
				path := filepath.Join(indexPath, entry.Name())
				if tc.corrupt {
					if strings.HasPrefix(entry.Name(), "MANIFEST-") {
						// Unlink before replacing so a possible checkpoint hard link cannot
						// modify the source database fixture.
						require.NoError(t, os.Remove(path))
						require.NoError(t, os.WriteFile(path, []byte("invalid manifest record contents"), 0o600))
						manifests++
					}
				} else if entry.Name() != ".ready" {
					require.NoError(t, os.RemoveAll(path))
				}
			}
			if tc.corrupt {
				require.Positive(t, manifests)
			}
			require.True(t, readstore.CheckpointDirReady(indexPath), "must reach the read-index open, not the earlier marker guard")
			main, index, err = impl.openCheckpointStores(context.Background(), id)
			require.Nil(t, main)
			require.Nil(t, index)
			var notFound *commonpb.NotFoundError
			switch {
			case tc.corrupt:
				require.Error(t, err)
				require.False(t, errors.As(err, &notFound), "deletion must not hide unrelated corruption")
				require.False(t, errors.Is(err, pebble.ErrDBDoesNotExist))
				require.ErrorContains(t, err, "opening checkpoint read index")
			case tc.deleted:
				require.ErrorAs(t, err, &notFound, "a committed deletion must not leak a raw Pebble error")
			default:
				require.False(t, errors.As(err, &notFound))
				if tc.removeDirectory {
					require.ErrorContains(t, err, "error opening database at")
					require.False(t, errors.Is(err, pebble.ErrDBDoesNotExist), "exercise Pebble's untyped directory-absence error")
				} else {
					require.ErrorIs(t, err, pebble.ErrDBDoesNotExist, "unexplained storage loss must remain visible")
				}
			}
			// The failed second open must release the main database's Pebble lock.
			reopened, reopenErr := dal.OpenReadOnly(impl.store.QueryCheckpointMainDir(id), impl.logger)
			require.NoError(t, reopenErr, "failed checkpoint open must close its main store")
			require.NoError(t, reopened.Close())
		})
	}
}

// Embedding the existing no-op logger keeps this synchronization scoped to the
// real Pebble open event without adding a production filesystem hook.
type checkpointDirectoryDeletingLogger struct {
	noopLogger

	once   sync.Once
	remove func()
}

func (*checkpointDirectoryDeletingLogger) Enabled(logging.Level) bool { return true }
func (l *checkpointDirectoryDeletingLogger) Tracef(string, ...any)    { l.once.Do(l.remove) }
