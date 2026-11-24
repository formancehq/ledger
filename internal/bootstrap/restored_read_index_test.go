package bootstrap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

func TestPrepareRestoredReadIndexHonorsWALBootstrapBoundary(t *testing.T) {
	t.Parallel()
	data, index := t.TempDir(), t.TempDir()
	logger := logging.NopZap()
	cfg := Config{DataDir: data, ReadIndexConfig: ReadIndexConfig{Dir: index}}
	cfg.RaftConfig.WalDir = t.TempDir()
	w, err := wal.New(cfg.RaftConfig.WalDir, logger, noop.NewMeterProvider().Meter("test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	old := node.RestoredMarker{LastAppliedIndex: 43}
	require.NoError(t, node.WriteRestoredMarker(data, old))
	require.NoError(t, w.CreateSnapshot(43, &raftpb.ConfState{Voters: []uint64{1}}, nil))
	_, err = prepareRestoredReadIndex(cfg, w)
	require.ErrorContains(t, err, "initialized Raft WAL and no live primary store")
	require.NoError(t, os.Mkdir(filepath.Join(data, "live"), 0o700))
	target := filepath.Join(index, "readindex")
	require.NoError(t, os.Mkdir(target, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(target, "00012.sst"), []byte("live index"), 0o600))
	_, err = prepareRestoredReadIndex(cfg, w)
	require.NoError(t, err)
	got, err := node.ReadRestoredMarker(data)
	require.NoError(t, err)
	require.Equal(t, &old, got)
	require.FileExists(t, filepath.Join(target, "00012.sst"))
}

func TestPrepareRestoredReadIndexUpgradesOldMarker(t *testing.T) {
	t.Parallel()
	data := t.TempDir()
	logger := logging.NopZap()
	cfg := Config{DataDir: data}
	cfg.RaftConfig.WalDir = t.TempDir()
	w, err := wal.New(cfg.RaftConfig.WalDir, logger, noop.NewMeterProvider().Meter("test"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	require.NoError(t, node.WriteRestoredMarker(data, node.RestoredMarker{LastAppliedIndex: 5}))
	_, err = prepareRestoredReadIndex(cfg, w)
	require.NoError(t, err)
	marker, err := node.ReadRestoredMarker(data)
	require.NoError(t, err)
	require.NotEmpty(t, marker.ReadIndexGeneration)
	target := filepath.Join(data, "read-indexes", "readindex")
	require.DirExists(t, target)
	_, err = prepareRestoredReadIndex(cfg, w)
	require.NoError(t, err)
	next, err := node.ReadRestoredMarker(data)
	require.NoError(t, err)
	require.Equal(t, marker, next)
}
