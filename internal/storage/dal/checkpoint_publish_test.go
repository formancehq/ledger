package dal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestScanLatestCheckpointIDIgnoresIncompleteCheckpoint(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	parent := filepath.Join(dataDir, checkpointsDir)
	complete := filepath.Join(parent, "1")
	incomplete := filepath.Join(parent, "2")
	require.NoError(t, os.MkdirAll(complete, 0o755))
	require.NoError(t, os.MkdirAll(incomplete, 0o755))
	require.NoError(t, MarkCheckpointReady(complete))

	id, found, err := ScanLatestCheckpointID(dataDir)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(1), id)

	require.NoError(t, os.RemoveAll(complete))
	_, _, err = ScanLatestCheckpointID(dataDir)
	require.ErrorContains(t, err, "no complete checkpoint")
}

func TestNewStoreKeepsLiveAfterInterruptedCheckpoint(t *testing.T) {
	t.Parallel()
	dataDir := t.TempDir()
	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")
	store, err := NewStore(dataDir, logger, meter, DefaultConfig())
	require.NoError(t, err)
	require.NoError(t, store.Close())
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, checkpointsDir, "1"), 0o755))

	reopened, err := NewStore(dataDir, logger, meter, DefaultConfig())
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func TestReconcileCheckpointReplacements(t *testing.T) {
	t.Parallel()
	for _, published := range []bool{false, true} {
		name := "interrupted_swap"
		if published {
			name = "completed_swap"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dataDir := t.TempDir()
			parent := filepath.Join(dataDir, checkpointsDir)
			previous := filepath.Join(parent, "3.previous")
			current := filepath.Join(parent, "3")
			require.NoError(t, os.MkdirAll(previous, 0o755))
			require.NoError(t, MarkCheckpointReady(previous))
			require.NoError(t, os.WriteFile(filepath.Join(previous, "source"), []byte("old"), 0o600))
			if published {
				require.NoError(t, os.MkdirAll(current, 0o755))
				require.NoError(t, MarkCheckpointReady(current))
				require.NoError(t, os.WriteFile(filepath.Join(current, "source"), []byte("new"), 0o600))
			}

			require.NoError(t, reconcileCheckpointReplacements(dataDir))
			value, err := os.ReadFile(filepath.Join(current, "source"))
			require.NoError(t, err)
			if published {
				require.Equal(t, "new", string(value))
			} else {
				require.Equal(t, "old", string(value))
			}
			require.NoDirExists(t, previous)
		})
	}
}
