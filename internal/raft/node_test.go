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
	spoolDir := t.TempDir()

	wal, err := wal.New(walDir, logging.Testing())
	require.NoError(t, err)

	spool, err := NewDefaultSpool(DefaultSpoolConfig{
		Dir: spoolDir,
	})
	require.NoError(t, err)

	store, err := pebble.NewStore(dataDir, logging.Testing(), noop.Meter{})
	require.NoError(t, err)

	interceptedWAL := NewMockWAL(ctrl)
	require.NoError(t, err)

	// Set all methods to passthrough
	interceptedWAL.EXPECT().
		Snapshot().
		AnyTimes().
		DoAndReturn(wal.Snapshot)

	interceptedWAL.EXPECT().
		Compact(gomock.Any()).
		AnyTimes().
		DoAndReturn(wal.Compact)

	interceptedWAL.EXPECT().
		FirstIndex().
		AnyTimes().
		DoAndReturn(wal.FirstIndex)

	interceptedWAL.EXPECT().
		LastIndex().
		AnyTimes().
		DoAndReturn(wal.LastIndex)

	interceptedWAL.EXPECT().
		InitialState().
		AnyTimes().
		DoAndReturn(wal.InitialState)

	interceptedWAL.EXPECT().
		Term(gomock.Any()).
		AnyTimes().
		DoAndReturn(wal.Term)

	interceptedWAL.EXPECT().
		Append(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(wal.Append)

	interceptedWAL.EXPECT().
		Entries(gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(wal.Entries)

	// Expect a first snapshot creation with no error
	interceptedWAL.EXPECT().
		CreateSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(wal.CreateSnapshot)

	node := newTestingNode(t, ctrl, spool, interceptedWAL, store)
	nodeError := startAndCatchError(node)

	require.Eventually(t, node.IsLeader, 2*time.Second, 10*time.Millisecond)

	_, err = node.CreateLedger(ctx, &ledgerpb.CreateLedgerCommand{
		Name: "default",
	})
	require.NoError(t, err)

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
		_, err := node.Apply(ctx, createTransaction())
		require.NoError(t, err)
	}

	var errUnexpected = errors.New("unexpected error")
	interceptedWAL.EXPECT().
		CreateSnapshot(uint64(10), gomock.Any(), gomock.Any()).
		DoAndReturn(func(u uint64, state *raftpb.ConfState, bytes []byte) error {
			return errUnexpected
		})

	// Now should trigger the snapshotting
	go func() {
		_, _ = node.Apply(ctx, createTransaction())
	}()
	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "node did not fail and it should")
	case err := <-nodeError:
		require.Error(t, err)
		require.ErrorIs(t, err, errUnexpected)
	}

	require.NoError(t, wal.Close())
	require.NoError(t, store.Close(ctx))
	require.NoError(t, spool.Close())

	// Start a new fresh node
	t.Log("Starting a new node")

	spool, err = NewDefaultSpool(DefaultSpoolConfig{
		Dir: spoolDir,
	})
	require.NoError(t, err)

	store, err = pebble.NewStore(dataDir, logging.Testing(), noop.Meter{})
	require.NoError(t, err)

	node = newTestingNode(t, ctrl, spool, wal, store)
	nodeError = startAndCatchError(node)

	require.Eventually(t, node.IsLeader, 2*time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		balances, err := node.store.GetBalances(logging.TestingContext(), "default", map[string][]string{
			"world": {"USD"},
		})
		require.NoError(t, err)

		worldBalance := balances["world"]["USD"]
		return worldBalance.Cmp(big.NewInt(-800)) == 0

	}, 2*time.Second, 1000*time.Millisecond)

}

func newTestingNode(t *testing.T, ctrl *gomock.Controller, spool Spool, wal WAL, store storepkg.Store) *Node {

	var (
		unreachableCh = make(<-chan uint64)
		recvCh        = make(<-chan raftpb.Message)
	)

	transport := NewMockTransport(ctrl)
	transport.EXPECT().
		Recv().
		Return(recvCh).
		AnyTimes()
	transport.EXPECT().
		Unreachable().
		Return(unreachableCh).
		AnyTimes()

	node, err := NewNode(
		NodeConfig{
			NodeID:            1,
			SnapshotThreshold: 10,
		},
		transport,
		store,
		logging.Testing(),
		noop.Meter{},
		spool,
		wal,
	)
	require.NoError(t, err)

	return node
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
