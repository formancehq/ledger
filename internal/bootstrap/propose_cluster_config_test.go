package bootstrap

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	commonpb "github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// seedClusterState persists a cluster state whose config matches the desired
// one, so proposeClusterConfigIfNeeded short-circuits at its equality check
// and never reaches the proposal path (which needs a live *node.Node).
func seedClusterState(t *testing.T, store *dal.Store, cfg *ledgerpb.ClusterConfig) {
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
	seedClusterState(t, store, &ledgerpb.ClusterConfig{RotationThreshold: 42})

	shared := &ledgerpb.ClusterConfig{}
	cfg := Config{
		BloomConfig: shared,
		RaftConfig:  node.NodeConfig{RotationThreshold: 42},
	}

	// The persisted state matches, so the call returns before touching the
	// node or the plan builder; nil is safe for both here.
	proposeClusterConfigIfNeeded(t.Context(), nil, nil, store, cfg, logging.Testing())

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
	seedClusterState(t, store, &ledgerpb.ClusterConfig{RotationThreshold: 42})

	shared := &ledgerpb.ClusterConfig{}
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
			proposeClusterConfigIfNeeded(t.Context(), nil, nil, store, cfg, logger)
		}()
	}

	wg.Wait()

	require.Zero(t, shared.GetRotationThreshold(),
		"concurrent LeaderReadyEvent deliveries must not mutate the shared cfg.BloomConfig")
}

// Hold FSM application after Raft acceptance, as can happen when HTTP failure
// stops the node while the production leader-ready observer is proposing config.
type pendingClusterConfigProposer struct {
	accepted chan context.Context
}

func (p pendingClusterConfigProposer) Propose(ctx context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	proposal.Resolve(nil, nil)
	p.accepted <- ctx

	return futures.New[state.ApplyResult](), nil
}

func TestProposeClusterConfigIfNeeded_CancelledGenerationJoinsObserver(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	c, err := cache.New(1000, noop.Meter{})
	require.NoError(t, err)
	logger := logging.Testing()
	builder := plan.NewBuilder(node.NewIndexTracker(1), c, attributes.New(), store, nil, logger, 0)

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		proposer := pendingClusterConfigProposer{accepted: make(chan context.Context, 1)}
		observer := node.NewObserver(func(event any) {
			e := event.(node.LeaderReadyEvent)
			proposeClusterConfigIfNeeded(e.Context, proposer, builder, store,
				Config{BloomConfig: &ledgerpb.ClusterConfig{}}, logger)
		})
		done := make(chan struct{})
		go func() {
			defer close(done)
			observer.Emit(node.LeaderReadyEvent{Context: ctx})
		}()
		proposalCtx := <-proposer.accepted
		synctest.Wait()
		require.NoError(t, proposalCtx.Err(), "proposal must be pending at cancellation")
		started := time.Now()
		cancel()
		<-done
		require.ErrorIs(t, proposalCtx.Err(), context.Canceled)
		require.Equal(t, time.Duration(0), time.Since(started),
			"joining the observer must not wait for the independent 30-second proposal timeout")
	})
}
