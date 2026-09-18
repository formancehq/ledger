package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A wire boundary lets the real driver observe one controlled response while
// successful mutations still reach the real Ledger node. This is not a model
// of the cap or checkpoint lifecycle.
type checkpointProxy struct {
	servicepb.UnimplementedBucketServiceServer
	clusterpb.UnimplementedClusterServiceServer
	apply func(context.Context, *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error)
	list  func(context.Context, *clusterpb.ListQueryCheckpointsRequest) (*clusterpb.ListQueryCheckpointsResponse, error)
	info  func(context.Context, *clusterpb.GetQueryCheckpointInfoRequest) (*clusterpb.QueryCheckpointInfo, error)
}

func (p *checkpointProxy) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	return p.apply(ctx, req)
}

func (p *checkpointProxy) ListQueryCheckpoints(ctx context.Context, req *clusterpb.ListQueryCheckpointsRequest) (*clusterpb.ListQueryCheckpointsResponse, error) {
	return p.list(ctx, req)
}

func (p *checkpointProxy) GetQueryCheckpointInfo(ctx context.Context, req *clusterpb.GetQueryCheckpointInfoRequest) (*clusterpb.QueryCheckpointInfo, error) {
	return p.info(ctx, req)
}

func serveCheckpointProxy(t *testing.T, proxy *checkpointProxy) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, proxy)
	clusterpb.RegisterClusterServiceServer(server, proxy)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-done)
	})
	return listener.Addr().String()
}

func requireCheckpointFinding(t *testing.T, assertions []sdkAssertion, message string) {
	t.Helper()
	for _, assertion := range assertions {
		if assertion.Message == message && assertion.DisplayType == "Unreachable" && !assertion.Condition {
			return
		}
	}
	t.Fatalf("driver must preserve finding %q", message)
}

func TestQueryCheckpointDriverRejectsOtherErrors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		code   codes.Code
		reason string
	}{
		{"unrelated precondition", codes.FailedPrecondition, domain.ErrReasonInsufficientFunds},
		{"precondition without reason", codes.FailedPrecondition, ""},
		{"wrong code for capacity", codes.Internal, domain.ErrReasonCheckpointLimitReached},
		{"internal", codes.Internal, ""},
		{"unknown", codes.Unknown, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			response := status.New(tc.code, "injected response")
			if tc.reason != "" {
				var err error
				response, err = response.WithDetails(&errdetails.ErrorInfo{Reason: tc.reason, Domain: "ledger"})
				require.NoError(t, err)
			}
			var calls atomic.Int32
			address := serveCheckpointProxy(t, &checkpointProxy{apply: func(context.Context, *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
				calls.Add(1)
				return nil, response.Err()
			}})
			assertions := runCheckpointDriver(t, address)
			requireCheckpointFinding(t, assertions, "CreateQueryCheckpoint should not fail")
			require.EqualValues(t, 1, calls.Load(), "permanent failures must not be retried as capacity pressure")
		})
	}
}

func TestQueryCheckpointDriverCleansOwnedCheckpointAfterReadFailure(t *testing.T) {
	t.Parallel()
	ctx, address, client, cluster := checkpointTestServer(t)
	var foreignIDs []uint64
	for range 9 {
		id, _, err := actions.CreateQueryCheckpoint(ctx, client)
		require.NoError(t, err)
		foreignIDs = append(foreignIDs, id)
	}
	for _, tc := range []struct {
		name             string
		listError        error
		omitList         bool
		infoError        error
		finding          string
		deleteError      bool
		cancelBeforeList bool
	}{
		{name: "list failure", listError: status.Error(codes.Internal, "injected list failure"), finding: "ListQueryCheckpoints should not fail"},
		{name: "lagging list", omitList: true},
		{name: "info failure", infoError: status.Error(codes.FailedPrecondition, "injected info failure"), finding: "GetQueryCheckpointInfo should not fail"},
		{name: "normal lifecycle"},
		{name: "canceled driver context", cancelBeforeList: true, finding: "ListQueryCheckpoints should not fail"},
		{name: "cleanup failure", listError: status.Error(codes.Internal, "injected list failure"), finding: "ListQueryCheckpoints should not fail", deleteError: true},
		{name: "normal delete failure", finding: "DeleteQueryCheckpoint should not fail", deleteError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var createdID atomic.Uint64
			var deletes atomic.Int32
			proxy := &checkpointProxy{
				apply: func(callCtx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
					for _, request := range req.GetUnsigned().GetRequests() {
						if deleted := request.GetDeleteQueryCheckpoint(); deleted != nil {
							deletes.Add(1)
							if deleted.GetCheckpointId() != createdID.Load() {
								return nil, status.Error(codes.Internal, "attempted to delete foreign checkpoint")
							}
							if tc.deleteError {
								return nil, status.Error(codes.Internal, "injected delete failure")
							}
						}
					}
					response, err := client.Apply(callCtx, req)
					if id, _, ok := actions.GetCreatedQueryCheckpoint(response); ok {
						createdID.Store(id)
					}
					return response, err
				},
				list: func(callCtx context.Context, req *clusterpb.ListQueryCheckpointsRequest) (*clusterpb.ListQueryCheckpointsResponse, error) {
					if tc.listError != nil {
						return nil, tc.listError
					}
					if tc.omitList {
						return &clusterpb.ListQueryCheckpointsResponse{}, nil
					}
					return cluster.ListQueryCheckpoints(callCtx, req)
				},
				info: func(callCtx context.Context, req *clusterpb.GetQueryCheckpointInfoRequest) (*clusterpb.QueryCheckpointInfo, error) {
					if tc.infoError != nil {
						return nil, tc.infoError
					}
					return cluster.GetQueryCheckpointInfo(callCtx, req)
				},
			}
			var extraEnv []string
			if tc.cancelBeforeList {
				extraEnv = append(extraEnv, "CHECKPOINT_DRIVER_CANCEL_BEFORE_LIST=1")
			}
			assertions := runCheckpointDriver(t, serveCheckpointProxy(t, proxy), extraEnv...)
			require.NotZero(t, createdID.Load(), "the create must reach the real node")
			if tc.finding != "" {
				requireCheckpointFinding(t, assertions, tc.finding)
			} else {
				requireNoCheckpointFindings(t, assertions)
			}
			require.EqualValues(t, 1, deletes.Load(), "exactly one delete must target the invocation's acknowledged checkpoint")
			if tc.deleteError {
				if tc.listError != nil {
					requireCheckpointFinding(t, assertions, "owned query checkpoint cleanup returned unexpected error")
				}
				// A rejected deletion stays visible and cannot promise reclamation.
				// The fixture now releases its known ID before testing healthy recovery.
				require.NoError(t, actions.DeleteQueryCheckpoint(ctx, client, createdID.Load()))
			}
			require.ElementsMatch(t, foreignIDs, listCheckpointIDs(t, ctx, cluster))
			// A clean invocation can fill the freed slot and release it again.
			recovery := runCheckpointDriver(t, address)
			requireNoCheckpointFindings(t, recovery)
			requireCheckpointEvent(t, recovery, "query checkpoint lifecycle completed")
		})
	}
}

func TestQueryCheckpointDriversCompeteForLastSlot(t *testing.T) {
	t.Parallel()
	ctx, address, client, cluster := checkpointTestServer(t)
	var foreignIDs []uint64
	for range 9 {
		id, _, err := actions.CreateQueryCheckpoint(ctx, client)
		require.NoError(t, err)
		foreignIDs = append(foreignIDs, id)
	}
	var creates, successes, rejections atomic.Int32
	createsFinished := make(chan struct{})
	proxyAddress := serveCheckpointProxy(t, &checkpointProxy{
		apply: func(callCtx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
			resp, err := client.Apply(callCtx, req)
			for _, request := range req.GetUnsigned().GetRequests() {
				if request.GetCreateQueryCheckpoint() != nil && creates.Add(1) == 2 {
					close(createsFinished)
				}
			}
			return resp, err
		},
		list: func(callCtx context.Context, req *clusterpb.ListQueryCheckpointsRequest) (*clusterpb.ListQueryCheckpointsResponse, error) {
			// Hold the successful owner's lifecycle before its delete, so the
			// second create necessarily competes for the same final slot.
			select {
			case <-createsFinished:
				return cluster.ListQueryCheckpoints(callCtx, req)
			case <-callCtx.Done():
				return nil, status.FromContextError(callCtx.Err()).Err()
			}
		},
		info: func(callCtx context.Context, req *clusterpb.GetQueryCheckpointInfoRequest) (*clusterpb.QueryCheckpointInfo, error) {
			return cluster.GetQueryCheckpointInfo(callCtx, req)
		},
	})
	t.Run("competing drivers", func(t *testing.T) {
		for _, name := range []string{"first", "second"} {
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				assertions := runCheckpointDriver(t, proxyAddress)
				requireNoCheckpointFindings(t, assertions)
				for _, assertion := range assertions {
					if assertion.Condition && assertion.Message == "query checkpoint lifecycle completed" {
						successes.Add(1)
					}
					if assertion.Condition && assertion.Message == "query checkpoint capacity reached" {
						rejections.Add(1)
					}
				}
			})
		}
	})
	require.EqualValues(t, 2, creates.Load())
	require.EqualValues(t, 1, successes.Load())
	require.EqualValues(t, 1, rejections.Load())
	recovery := runCheckpointDriver(t, address)
	requireNoCheckpointFindings(t, recovery)
	requireCheckpointEvent(t, recovery, "query checkpoint lifecycle completed")
	require.ElementsMatch(t, foreignIDs, listCheckpointIDs(t, ctx, cluster))
}
