package grpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

type checkpointWaitObservedContext struct {
	context.Context

	once     sync.Once
	observed chan struct{}
}

func (c *checkpointWaitObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })

	return c.Context.Done()
}

type checkpointAuthenticatedPeer struct{ *BucketServiceServerImpl }

func (s checkpointAuthenticatedPeer) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	return s.BucketServiceServerImpl.Apply(internalauth.WithClusterInternal(ctx, true), req)
}

func TestApplyCheckpointReplayWhileOriginalWaits(t *testing.T) {
	t.Parallel()
	for _, forwarded := range []bool{false, true} {
		name := "direct"
		if forwarded {
			name = "forwarded"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			leader, mockCtrl := newCheckpointWaitHarness(t)
			logs := []*commonpb.Log{{Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{CheckpointId: 1, MaxSequence: 7}}}}}
			gomock.InOrder(
				mockCtrl.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{logs[0].CloneVT()}}, nil),
				mockCtrl.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{logs[0].CloneVT()}, Replayed: true}, nil),
			)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			observed := &checkpointWaitObservedContext{Context: ctx, observed: make(chan struct{})}
			originalDone := make(chan error, 1)
			request := func() *servicepb.ApplyRequest {
				return servicepb.UnsignedApplyRequest("same-key", &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}})
			}
			go func() { _, err := leader.Apply(observed, request()); originalDone <- err }()
			select {
			case <-observed.observed:
			case <-ctx.Done():
				t.Fatal("original never entered readiness wait")
			}
			serving := leader
			if forwarded {
				follower, _ := newCheckpointWaitHarness(t)
				follower.ctrl = NewLedgerGrpcClient(dialBucketServer(t, checkpointAuthenticatedPeer{leader}))
				serving = follower
			}
			response, err := serving.Apply(ctx, request())
			require.NoError(t, err)
			require.Len(t, response.GetLogs(), len(logs))
			require.True(t, proto.Equal(logs[0], response.GetLogs()[0]))
			require.False(t, readstore.CheckpointDirReady(serving.store.QueryCheckpointReadIndexDir(1)))
			select {
			case err := <-originalDone:
				t.Fatalf("original returned before materialization: %v", err)
			default:
			}
			require.NoError(t, readstore.MarkCheckpointReady(mkdirAllForCheckpoint(t, leader.store.QueryCheckpointReadIndexDir(1))))
			leader.readStore.NotifyProgress()
			select {
			case err := <-originalDone:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("original did not return after materialization")
			}
		})
	}
}

func TestQueryCheckpointDeletedDistinguishesFollowerLag(t *testing.T) {
	t.Parallel()
	impl, _ := newCheckpointWaitHarness(t)
	deleted, err := impl.queryCheckpointDeleted(1)
	require.NoError(t, err)
	require.False(t, deleted, "missing before local creation is replication lag")
	// Test-only fixture writes emulate the atomic FSM create and delete batches.
	batch := impl.store.OpenWriteSession()
	require.NoError(t, state.SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: 1}))
	require.NoError(t, state.StoreNextQueryCheckpointID(batch, 2))
	require.NoError(t, batch.Commit())
	deleted, err = impl.queryCheckpointDeleted(1)
	require.NoError(t, err)
	require.False(t, deleted, "allocated checkpoint remains live")
	batch = impl.store.OpenWriteSession()
	require.NoError(t, state.DeleteQueryCheckpointFromBatch(batch, 1))
	require.NoError(t, batch.Commit())
	deleted, err = impl.queryCheckpointDeleted(1)
	require.NoError(t, err)
	require.True(t, deleted, "absent previously allocated checkpoint was deleted")
}

func TestApplyCheckpointDeletedWhileFreshCreationWaits(t *testing.T) {
	t.Parallel()
	impl, mockCtrl := newCheckpointWaitHarness(t)
	// Fixture the committed creation before entering its post-commit readiness wait.
	batch := impl.store.OpenWriteSession()
	require.NoError(t, state.SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: 1}))
	require.NoError(t, state.StoreNextQueryCheckpointID(batch, 2))
	require.NoError(t, batch.Commit())
	log := &commonpb.Log{Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{CheckpointId: 1, MaxSequence: 7}}}}
	mockCtrl.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{log.CloneVT()}}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observed := &checkpointWaitObservedContext{Context: ctx, observed: make(chan struct{})}
	type applyOutcome struct {
		response *servicepb.ApplyResponse
		err      error
	}
	done := make(chan applyOutcome, 1)
	go func() {
		response, err := impl.Apply(observed, servicepb.UnsignedApplyRequest("delete-during-wait", &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}}))
		done <- applyOutcome{response, err}
	}()
	select {
	case <-observed.observed:
	case <-ctx.Done():
		t.Fatal("fresh creation never entered readiness wait")
	}
	select {
	case result := <-done:
		t.Fatalf("creation returned before readiness or deletion: %v", result.err)
	default:
	}
	// Commit deletion without notifying read-index progress: an unavailable or
	// stalled builder must not prevent the original committed success returning.
	batch = impl.store.OpenWriteSession()
	require.NoError(t, state.DeleteQueryCheckpointFromBatch(batch, 1))
	require.NoError(t, batch.Commit())
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.Len(t, result.response.GetLogs(), 1)
		require.True(t, proto.Equal(log, result.response.GetLogs()[0]))
	case <-ctx.Done():
		t.Fatal("deletion did not release fresh creation wait")
	}
	require.False(t, readstore.CheckpointDirReady(impl.store.QueryCheckpointReadIndexDir(1)))
}
