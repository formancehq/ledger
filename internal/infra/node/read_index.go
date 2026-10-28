package node

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/raft/v3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

var readIndexTracer = otel.Tracer("node.read_index")

// readIndexRequest wraps a Future that will be resolved with the commit index
// when the Raft leader confirms the ReadIndex request.
type readIndexRequest struct {
	future *futures.Future[uint64]
}

// makeReadIndexContext creates an opaque identity for one read invocation.
// It must remain unique across process restarts: an unchanged leader can retain
// a pre-restart request and return its older commit index in the same term.
func makeReadIndexContext() []byte {
	id := uuid.New()

	return id[:]
}

// ReadIndex sends a ReadIndex request through Raft and returns the current commit index.
// The returned index can be used with WaitForApplied to ensure the local FSM is fresh
// enough for a linearizable read.
func (node *Node) ReadIndex(ctx context.Context) (uint64, error) {
	rctx := makeReadIndexContext()
	reqID := string(rctx)

	req := &readIndexRequest{
		future: futures.New[uint64](),
	}
	node.pendingReads.Store(reqID, req)

	if err := node.execClusterCommand(ctx, false, func() error {
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

	commitIndex, err := req.future.Wait(ctx)
	if err != nil {
		node.pendingReads.Delete(reqID)

		return 0, err
	}

	return commitIndex, nil
}

// ReadBarrierInfo holds diagnostic data from a ReadIndex+WaitForApplied cycle.
type ReadBarrierInfo struct {
	CommitIndex    uint64
	PersistedAfter uint64
}

// ReadIndexAndWait performs a linearizable read barrier: it sends a ReadIndex request,
// waits for the Raft quorum to confirm, then waits for the local FSM to catch up.
// After this method returns, any subsequent read from the local store is guaranteed to
// reflect all writes committed before the ReadIndex call.
//
// Returns ErrNodeSyncing if the node is still catching up with the cluster.
// Callers (e.g. RoutedController) should forward the read to the leader in that case.
func (node *Node) ReadIndexAndWait(ctx context.Context) (*ReadBarrierInfo, error) {
	ctx, span := readIndexTracer.Start(ctx, "node.read_index_and_wait")
	defer span.End()

	// Only reject reads when the node is genuinely out of sync (restoring a
	// remote snapshot or waiting for a leader). During local snapshotting the
	// store is frozen but consistent — ReadIndex still works and WaitForApplied
	// will block until the spool is replayed, which is the correct behavior
	// (the read waits instead of failing immediately).
	if node.applier.IsSyncing() || node.applier.Status() == statusOutOfSync {
		return nil, ErrNodeSyncing
	}

	if node.GetLeader() == 0 {
		return nil, commonpb.ErrNoLeader
	}

	start := time.Now()

	ctx, riSpan := readIndexTracer.Start(ctx, "node.read_index_quorum")
	commitIndex, err := node.ReadIndex(ctx)

	riSpan.End()

	if err != nil {
		return nil, err
	}

	persistedBefore := node.fsm.LastPersistedIndex()
	span.SetAttributes(
		attribute.Int64("commit_index", int64(commitIndex)),
		attribute.Int64("persisted_before_wait", int64(persistedBefore)),
		attribute.String("leader", strconv.FormatUint(node.GetLeader(), 10)),
		attribute.String("node_id", strconv.FormatUint(node.config.NodeID, 10)),
	)

	_, waitSpan := readIndexTracer.Start(ctx, "node.wait_for_applied",
		trace.WithAttributes(attribute.Int64("target_index", int64(commitIndex))))
	if err := node.fsm.WaitForApplied(ctx, commitIndex); err != nil {
		waitSpan.End()

		return nil, err
	}

	persistedAfter := node.fsm.LastPersistedIndex()
	waitSpan.SetAttributes(attribute.Int64("persisted_after_wait", int64(persistedAfter)))
	waitSpan.End()

	if node.readIndexDurationHistogram != nil {
		node.readIndexDurationHistogram.Record(ctx, time.Since(start).Seconds())
	}

	return &ReadBarrierInfo{
		CommitIndex:    commitIndex,
		PersistedAfter: persistedAfter,
	}, nil
}

// failAllPendingReads resolves all pending ReadIndex requests with the given error.
// Called on leadership loss to unblock waiting callers.
func (node *Node) failAllPendingReads(err error) {
	node.pendingReads.Range(func(id string, req *readIndexRequest) bool {
		req.future.Resolve(0, err)
		node.pendingReads.Delete(id)

		return true
	})
}
