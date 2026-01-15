package raft

import (
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	storepkg "github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/formancehq/ledger-v3-poc/internal/store/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/wal"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
)

func TestNodeFailureBetweenStoreSnapshotAndWalSnapshot(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	ctx := logging.TestingContext()

	walDir := t.TempDir()
	dataDir := t.TempDir()

	node, spiedWAL, wal, _, store := newTestingNode(t, ctrl, walDir, dataDir)
	nodeError := startAndCatchError(node)

	require.Eventually(t, node.IsLeader, 2*time.Second, 10*time.Millisecond)

	_, err := node.CreateLedger(ctx, &ledgerpb.CreateLedgerCommand{
		Name: "default",
	})
	require.NoError(t, err)

	var errUnexpected = errors.New("unexpected error")
	spiedWAL.EXPECT().
		CreateSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(u uint64, state *raftpb.ConfState, bytes []byte) error {
			return errUnexpected
		})

	createTransaction := func() *ledgerpb.Command {
		return NewCreateLogCommand(&ledgerpb.CommandInput{
			Command: &ledgerpb.CommandInput_AppendTransaction{
				AppendTransaction: &ledgerpb.AppendTransactionCommand{
					Postings: []*ledgerpb.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      ledgerpb.NewBigInt(big.NewInt(100)),
						Asset:       "USD",
					}},
				},
			},
		}, "default", nil)
	}

	// Should not trigger any snapshotting at this point
	for range 7 {
		_, err := node.Apply(createTransaction(), time.Second)
		require.NoError(t, err)
	}

	// Now should trigger the snapshotting
	go func() {
		_, _ = node.Apply(createTransaction(), time.Second)
	}()
	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "node did not fail")
	case err := <-nodeError:
		require.Error(t, err)
		require.ErrorIs(t, err, errUnexpected)
	}

	require.NoError(t, wal.Close())
	require.NoError(t, store.Close(ctx))

	// Start a new fresh node
	t.Log("Starting a new node")
	node, spiedWAL, wal, _, store = newTestingNode(t, ctrl, walDir, dataDir)
	nodeError = startAndCatchError(node)

	require.Eventually(t, node.IsLeader, 2*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		balances, err := store.GetBalances(logging.TestingContext(), "default", map[string][]string{
			"world": {"USD"},
		})
		require.NoError(t, err)

		worldBalance := balances["world"]["USD"]
		return worldBalance.Cmp(big.NewInt(-800)) == 0

	}, 2*time.Second, 1000*time.Millisecond)

}

func newTestingNode(t *testing.T, ctrl *gomock.Controller, walDir, dataDir string) (*Node, *MockWAL, *wal.WAL, *storepkg.MockStore, *pebble.Store) {

	var (
		unreachableCh = make(<-chan uint64)
		recvCh        = make(<-chan raftpb.Message)
	)

	spool := NewMockSpool(ctrl)
	transport := NewMockTransport(ctrl)
	transport.EXPECT().
		Recv().
		Return(recvCh).
		AnyTimes()
	transport.EXPECT().
		Unreachable().
		Return(unreachableCh).
		AnyTimes()

	spiedWAL, wal := buildSpiedWAL(t, ctrl, walDir)
	spiedStore, store := buildSpiedStore(t, ctrl, dataDir)

	node, err := NewNode(
		NodeConfig{
			NodeID:            1,
			SnapshotThreshold: 10,
		},
		transport,
		spiedStore,
		logging.Testing(),
		noop.Meter{},
		spool,
		spiedWAL,
	)
	require.NoError(t, err)

	return node, spiedWAL, wal, spiedStore, store
}

func buildSpiedWAL(t *testing.T, ctrl *gomock.Controller, location string) (*MockWAL, *wal.WAL) {
	wal, err := wal.New(location, logging.Testing())
	require.NoError(t, err)

	spiedWAL := NewMockWAL(ctrl)
	require.NoError(t, err)

	spiedWAL.EXPECT().
		Snapshot().
		AnyTimes().
		DoAndReturn(wal.Snapshot)

	spiedWAL.EXPECT().
		CreateSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(wal.CreateSnapshot)

	spiedWAL.EXPECT().
		Compact(gomock.Any()).
		AnyTimes().
		DoAndReturn(wal.Compact)

	spiedWAL.EXPECT().
		FirstIndex().
		AnyTimes().
		DoAndReturn(wal.FirstIndex)

	spiedWAL.EXPECT().
		LastIndex().
		AnyTimes().
		DoAndReturn(wal.LastIndex)

	spiedWAL.EXPECT().
		InitialState().
		AnyTimes().
		DoAndReturn(wal.InitialState)

	spiedWAL.EXPECT().
		Term(gomock.Any()).
		AnyTimes().
		DoAndReturn(wal.Term)

	spiedWAL.EXPECT().
		Append(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(wal.Append)

	spiedWAL.EXPECT().
		Entries(gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(wal.Entries)

	return spiedWAL, wal
}

func buildSpiedStore(t *testing.T, ctrl *gomock.Controller, location string) (*storepkg.MockStore, *pebble.Store) {
	store, err := pebble.NewStore(location, logging.Testing(), noop.Meter{})
	require.NoError(t, err)

	spiedStore := storepkg.NewMockStore(ctrl)
	require.NoError(t, err)

	spiedStore.EXPECT().
		CreateSnapshot(gomock.Any()).
		AnyTimes().
		DoAndReturn(store.CreateSnapshot)

	spiedStore.EXPECT().
		AppendLogs(gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(store.AppendLogs)

	spiedStore.EXPECT().
		GetLastAppliedIndex().
		AnyTimes().
		DoAndReturn(store.GetLastAppliedIndex)

	spiedStore.EXPECT().
		GetLastLogID(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(store.GetLastLogID)

	return spiedStore, store
}

func startAndCatchError(node *Node) chan error {
	nodeError := make(chan error, 1)
	go func() {
		defer func() {
			if e := recover(); e != nil {
				switch e := e.(type) {
				case error:
					nodeError <- fmt.Errorf("node start error: %w", e)
				default:
					nodeError <- fmt.Errorf("node start error: %v", e)
				}
			}
		}()
		nodeError <- node.Start(logging.TestingContext())
	}()
	return nodeError
}
