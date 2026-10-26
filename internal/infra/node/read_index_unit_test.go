package node

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"

	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

func TestReadIndexResolvesOnlyCompleteContext(t *testing.T) {
	t.Parallel()

	rctx := makeReadIndexContext()
	changedPrefix := bytes.Clone(rctx)
	changedPrefix[0] ^= 1
	changedSuffix := bytes.Clone(rctx)
	changedSuffix[len(changedSuffix)-1] ^= 1

	n := &Node{pendingReads: &SyncMap[string, *readIndexRequest]{}}
	waiter := futures.New[uint64]()
	n.pendingReads.Store(string(rctx), &readIndexRequest{future: waiter})
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()

	// Shared prefixes or suffixes, legacy counters and malformed lengths must
	// not be accepted as a certificate for this invocation.
	for _, other := range [][]byte{nil, {}, rctx[:8], append(bytes.Clone(rctx), 0), changedPrefix, changedSuffix} {
		n.resolveReadyReadStates(raft.Ready{ReadStates: []raft.ReadState{{Index: 2, RequestCtx: other}}})
		_, err := waiter.Wait(cancelled)
		require.ErrorIs(t, err, context.Canceled)
	}

	n.resolveReadyReadStates(raft.Ready{ReadStates: []raft.ReadState{{Index: 3, RequestCtx: rctx}}})
	index, err := waiter.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(3), index)

	// A duplicate response must not replace the completed result.
	n.resolveReadyReadStates(raft.Ready{ReadStates: []raft.ReadState{{Index: 4, RequestCtx: rctx}}})
	index, err = waiter.Wait(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(3), index)
}

func TestFailAllPendingReads(t *testing.T) {
	t.Parallel()

	n := &Node{
		pendingReads: &SyncMap[string, *readIndexRequest]{},
	}

	// Store some pending reads
	f1 := futures.New[uint64]()
	f2 := futures.New[uint64]()
	f3 := futures.New[uint64]()

	n.pendingReads.Store("first", &readIndexRequest{future: f1})
	n.pendingReads.Store("second", &readIndexRequest{future: f2})
	n.pendingReads.Store("third", &readIndexRequest{future: f3})

	testErr := errors.New("leadership lost")
	n.failAllPendingReads(testErr)

	// All futures should be resolved with the error
	_, err := f1.Wait(t.Context())
	require.ErrorIs(t, err, testErr)

	_, err = f2.Wait(t.Context())
	require.ErrorIs(t, err, testErr)

	_, err = f3.Wait(t.Context())
	require.ErrorIs(t, err, testErr)

	// pendingReads should be empty
	count := 0
	n.pendingReads.Range(func(_ string, _ *readIndexRequest) bool {
		count++

		return true
	})

	require.Zero(t, count)
}

func TestFailAllPendingReads_Empty(t *testing.T) {
	t.Parallel()

	n := &Node{
		pendingReads: &SyncMap[string, *readIndexRequest]{},
	}

	// Should not panic on empty map
	n.failAllPendingReads(errors.New("no pending reads"))
}

// Drive the actual ReadIndex entry point through its orchestrator command,
// rather than manufacturing the pending waiter or request context.
func TestReadIndexLifecycle(t *testing.T) {
	t.Parallel()

	for _, outcome := range []string{"success", "cancel before dispatch", "cancel after dispatch", "no leader"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				h := newReadIndexRestartHarness(t)
				if outcome != "no leader" {
					require.NoError(t, h.nodes[1].Campaign())
					h.drain(nil)
				}
				n := &Node{
					rawNode:          h.nodes[1],
					pendingReads:     &SyncMap[string, *readIndexRequest]{},
					clusterCommandCh: make(chan *clusterCommand),
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if outcome == "cancel before dispatch" {
					cancel()
				}
				type result struct {
					index uint64
					err   error
				}
				completed := make(chan result, 1)
				go func() {
					index, err := n.ReadIndex(ctx)
					completed <- result{index: index, err: err}
				}()
				if outcome != "cancel before dispatch" {
					cmd := <-n.clusterCommandCh
					cmd.errCh <- cmd.fn()
					switch outcome {
					case "success":
						h.drain(nil)
						require.Len(t, h.readStates[1], 1)
						require.Len(t, h.readStates[1][0].RequestCtx, 16)
						n.resolveReadyReadStates(raft.Ready{ReadStates: h.readStates[1]})
					case "cancel after dispatch":
						synctest.Wait()
						cancel()
					}
				}
				got := <-completed
				switch outcome {
				case "success":
					require.NoError(t, got.err)
					require.Equal(t, h.nodes[1].Status().GetCommit(), got.index)
				case "no leader":
					require.ErrorIs(t, got.err, ErrNotLeader)
				default:
					require.ErrorIs(t, got.err, context.Canceled)
				}
				n.pendingReads.Range(func(_ string, _ *readIndexRequest) bool {
					t.Error("completed read retained a pending waiter")

					return true
				})
			})
		})
	}
}
