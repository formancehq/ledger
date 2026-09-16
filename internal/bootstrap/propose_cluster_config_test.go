package bootstrap

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// seedClusterState persists a cluster state whose config matches the desired
// one, so proposeClusterConfigIfNeeded short-circuits at its equality check
// and never reaches the proposal path (which needs a live *node.Node).
func seedClusterState(t *testing.T, store *dal.Store, cfg *commonpb.ClusterConfig) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetProto(
		[]byte{dal.ZoneGlobal, dal.SubGlobClusterConfig},
		&commonpb.PersistedClusterState{Config: cfg},
	))
	require.NoError(t, batch.Commit())
}

// TestProposeClusterConfigIfNeeded_DoesNotMutateSharedConfig pins EN-2072 bug 1.
//
// cfg.BloomConfig is the single process-wide message provided by fx; building
// the proposal payload must clone it rather than write through the pointer.
// Before the fix, RotationThreshold was assigned in place, so the shared
// message observably changed under the caller.
func TestProposeClusterConfigIfNeeded_DoesNotMutateSharedConfig(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	seedClusterState(t, store, &commonpb.ClusterConfig{RotationThreshold: 42})

	shared := &commonpb.ClusterConfig{}
	cfg := Config{
		BloomConfig: shared,
		RaftConfig:  node.NodeConfig{RotationThreshold: 42},
	}

	// The persisted state matches, so the call returns before touching the
	// node or the plan builder; nil is safe for both here.
	proposeClusterConfigIfNeeded(nil, nil, store, cfg, logging.Testing())

	require.Zero(t, shared.GetRotationThreshold(),
		"proposeClusterConfigIfNeeded must not write RotationThreshold through the shared cfg.BloomConfig pointer")
}

// TestProposeClusterConfigIfNeeded_ConcurrentCallsDoNotRace reproduces the
// reported failure mode directly: LeaderReadyEvent is dispatched inline by the
// observer, and a leadership gain spawns a goroutine per gain with no term
// guard, so two deliveries can run this function concurrently. Under -race the
// in-place write raced the equality read on the same field.
func TestProposeClusterConfigIfNeeded_ConcurrentCallsDoNotRace(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	seedClusterState(t, store, &commonpb.ClusterConfig{RotationThreshold: 42})

	shared := &commonpb.ClusterConfig{}
	cfg := Config{
		BloomConfig: shared,
		RaftConfig:  node.NodeConfig{RotationThreshold: 42},
	}
	logger := logging.Testing()

	const concurrency = 8

	var wg sync.WaitGroup

	wg.Add(concurrency)

	for range concurrency {
		go func() {
			defer wg.Done()
			proposeClusterConfigIfNeeded(nil, nil, store, cfg, logger)
		}()
	}

	wg.Wait()

	require.Zero(t, shared.GetRotationThreshold(),
		"concurrent LeaderReadyEvent deliveries must not mutate the shared cfg.BloomConfig")
}
