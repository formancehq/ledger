package server

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/infra/incarnationmigration"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestMigrateIncarnationsRequiresExistingStore(t *testing.T) {
	cmd := NewMigrateIncarnationsCommand()
	cmd.SetArgs([]string{"--data-dir", filepath.Join(t.TempDir(), "missing")})
	require.ErrorContains(t, cmd.Execute(), "opening existing Ledger data store")
}

func TestMigrateIncarnationsRunsWithoutServer(t *testing.T) {
	dir := t.TempDir()
	store, err := dal.NewStore(dir, logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{Revision: 1, CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.AccountIncarnationWritesV1), Active: true}}}))
	require.NoError(t, batch.Commit())
	require.NoError(t, store.Close())

	cmd := NewMigrateIncarnationsCommand()
	cmd.SetArgs([]string{"--data-dir", dir})
	require.ErrorContains(t, cmd.Execute(), "requires an active cluster drain")
	store, err = dal.NewStore(dir, logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	writeMigrationDrain(t, store)
	require.NoError(t, store.Close())
	cmd = NewMigrateIncarnationsCommand()
	cmd.SetArgs([]string{"--data-dir", dir})
	require.NoError(t, cmd.Execute())

	store, err = dal.NewStore(dir, logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	marker, err := incarnationmigration.ReadMarker(store)
	require.NoError(t, err)
	require.True(t, marker.Complete)
}

func TestMigrateIncarnationsAcceptsCheckpointOnlyStore(t *testing.T) {
	dir := t.TempDir()
	store, err := dal.NewStore(dir, logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{Revision: 1, CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(capabilities.AccountIncarnationWritesV1), Active: true}}}))
	require.NoError(t, batch.Commit())
	writeMigrationDrain(t, store)
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "checkpoints"), 0o755))
	require.NoError(t, store.Checkpoint(filepath.Join(dir, "checkpoints", "1")))
	require.NoError(t, store.Close())
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "live")))

	cmd := NewMigrateIncarnationsCommand()
	cmd.SetArgs([]string{"--data-dir", dir})
	require.NoError(t, cmd.Execute())
	store, err = dal.NewStore(dir, logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	marker, err := incarnationmigration.ReadMarker(store)
	require.NoError(t, err)
	require.True(t, marker.Complete)
}

func writeMigrationDrain(t *testing.T, store *dal.Store) {
	t.Helper()
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneClusterTransient, dal.SubTransientDrainState}, &raftcmdpb.DrainState{Epoch: "migration", Index: 1, Members: []uint64{1}}))
	var applied [8]byte
	binary.BigEndian.PutUint64(applied[:], 1)
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneClusterPersistent, dal.SubGlobLastAppliedIndex}, applied[:]))
	require.NoError(t, batch.Commit())
}

func TestMigrateIncarnationsRejectsPositionalPath(t *testing.T) {
	cmd := NewMigrateIncarnationsCommand()
	cmd.SetArgs([]string{"/some/other/data"})
	require.ErrorContains(t, cmd.Execute(), "unknown command")
}
