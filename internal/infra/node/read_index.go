package node

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/futures"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

var readIndexTracer = otel.Tracer("node.read_index")

// readIndexRequest wraps a Future that will be resolved with the commit index
// when the Raft leader confirms the ReadIndex request.
type readIndexRequest struct {
	future *futures.Future[uint64]
}

// nextReadIndexID is a monotonically increasing counter used to generate unique
// 8-byte request context (rctx) values for ReadIndex requests.
var nextReadIndexID atomic.Uint64

// makeReadIndexContext creates a unique 8-byte context for a ReadIndex request.
func makeReadIndexContext(id uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], id)

	return buf[:]
}

// parseReadIndexContext extracts the request ID from an 8-byte ReadIndex context.
func parseReadIndexContext(rctx []byte) (uint64, bool) {
	if len(rctx) != 8 {
		return 0, false
	}

	return binary.BigEndian.Uint64(rctx), true
}

// ReadIndex sends a ReadIndex request through Raft and returns the current commit index.
// The returned index can be used with WaitForApplied to ensure the local FSM is fresh
// enough for a linearizable read.
func (node *Node) ReadIndex(ctx context.Context) (uint64, error) {
	reqID := nextReadIndexID.Add(1)
	rctx := makeReadIndexContext(reqID)

	req := &readIndexRequest{
		future: futures.New[uint64](),
	}
	node.pendingReads.Store(reqID, req)

	if err := node.execClusterCommand(ctx, func() error {
		// Guard against dispatching ReadIndex when the node is a follower with no
		// known leader. In that case etcd-raft's stepFollower silently drops the
		// request and the future would never be resolved, hanging the caller.
		st := node.rawNode.Status()
		if st.RaftState != raft.StateLeader && st.Lead == 0 {
			return ErrNotLeader
		}

		node.rawNode.ReadIndex(rctx)

		return nil
	}); err != nil {
		node.pendingReads.Delete(reqID)

		return 0, fmt.Errorf("dispatching ReadIndex: %w", err)
	}

	commitIndex, err := req.future.WaitContext(ctx)
	if err != nil {
		node.pendingReads.Delete(reqID)

		return 0, err
	}

	return commitIndex, nil
}

// ReadIndexAndWait performs a linearizable read barrier: it sends a ReadIndex request,
// waits for the Raft quorum to confirm, then waits for the local FSM to catch up.
// After this method returns, any subsequent read from the local store is guaranteed to
// reflect all writes committed before the ReadIndex call.
//
// Returns ErrNodeSyncing if the node is still catching up with the cluster.
// Callers (e.g. RoutedController) should forward the read to the leader in that case.
func (node *Node) ReadIndexAndWait(ctx context.Context) error {
	ctx, span := readIndexTracer.Start(ctx, "node.read_index_and_wait")
	defer span.End()

	if node.isSyncing() {
		return ErrNodeSyncing
	}

	if node.GetLeader() == 0 {
		return commonpb.ErrNoLeader
	}

	start := time.Now()

	ctx, riSpan := readIndexTracer.Start(ctx, "node.read_index_quorum")
	commitIndex, err := node.ReadIndex(ctx)

	riSpan.End()

	if err != nil {
		return err
	}

	span.SetAttributes(attribute.Int64("commit_index", int64(commitIndex)))

	_, waitSpan := readIndexTracer.Start(ctx, "node.wait_for_applied",
		trace.WithAttributes(attribute.Int64("target_index", int64(commitIndex))))
	if err := node.fsm.WaitForApplied(ctx, commitIndex); err != nil {
		waitSpan.End()

		return err
	}

	waitSpan.End()

	if node.readIndexDurationHistogram != nil {
		node.readIndexDurationHistogram.Record(ctx, time.Since(start).Microseconds())
	}

	return nil
}

// failAllPendingReads resolves all pending ReadIndex requests with the given error.
// Called on leadership loss to unblock waiting callers.
func (node *Node) failAllPendingReads(err error) {
	node.pendingReads.Range(func(id uint64, req *readIndexRequest) bool {
		req.future.Resolve(0, err)
		node.pendingReads.Delete(id)

		return true
	})
}

