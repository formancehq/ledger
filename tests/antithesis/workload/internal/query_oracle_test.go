package internal_test

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	workload "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
)

const permanentOracleError = "query oracle encountered a permanent setup or read error"

func TestQueryOracleReadErrors(t *testing.T) {
	for _, tc := range []struct {
		name         string
		code         codes.Code
		open         bool
		missingIndex bool
		cancelCaller bool
		wantFinding  bool
	}{
		{name: "real missing index", code: codes.FailedPrecondition, missingIndex: true, wantFinding: true},
		{name: "permanent stream open", code: codes.InvalidArgument, open: true, wantFinding: true},
		{name: "permanent after partial row", code: codes.Unknown, wantFinding: true},
		{name: "transient stream open", code: codes.Unavailable, open: true},
		{name: "transient after partial row", code: codes.Unavailable},
		{name: "uncancelled caller", code: codes.Canceled, wantFinding: true},
		{name: "caller cancellation", code: codes.Canceled, open: true, cancelCaller: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			drivertest.CheckEmissions(t, func() {
				ctx, client := drivertest.StartServer(t)
				if tc.missingIndex {
					require.NoError(t, workload.CreateLedger(ctx, client, "oracle"))
				} else {
					seedOracle(t, ctx, client)
				}
				var reader servicepb.BucketServiceClient = client
				if !tc.missingIndex {
					reader = &faultedQueryClient{BucketServiceClient: client, failure: status.Error(tc.code, "injected read failure"), open: tc.open}
				}
				if tc.cancelCaller {
					var cancel context.CancelFunc
					ctx, cancel = context.WithCancel(ctx)
					cancel()
				}
				ids, err := workload.ReadOracleTransactions(ctx, reader, "oracle", actions.ReferenceFilter("reference"))
				require.Equal(t, tc.code, status.Code(err))
				require.Nil(t, ids, "an incomplete page must not certify any business result")
				if tc.missingIndex {
					require.ErrorContains(t, err, "index not found: reference")
				}
			}, func(records []drivertest.Assertion) {
				found := false
				for _, record := range records {
					if record.Hit && record.Message == permanentOracleError {
						found = true
						require.False(t, record.Condition)
						require.Equal(t, "oracle", record.Details["ledger"])
						require.Equal(t, tc.code.String(), record.Details["code"])
						require.NotEmpty(t, record.Details["error"])
					}
				}
				require.Equal(t, tc.wantFinding, found)
			})
		})
	}
}

func TestQueryOracleRetriesIndexBuilding(t *testing.T) {
	drivertest.CheckEmissions(t, func() {
		ctx, client := drivertest.StartServer(t)
		expected := seedOracle(t, ctx, client)
		building, err := status.New(codes.Unavailable, "index still building").WithDetails(&errdetails.ErrorInfo{
			Domain: "ledger", Reason: domain.ErrReasonIndexBuilding,
		})
		require.NoError(t, err)
		reader := &faultedQueryClient{BucketServiceClient: client, failure: building.Err(), once: true}
		ids, err := workload.ReadOracleTransactions(ctx, reader, "oracle", actions.ReferenceFilter("reference"))
		require.NoError(t, err)
		require.Equal(t, expected, ids, "discard the partial row from the first attempt")
		require.Equal(t, 2, reader.calls, "exercise a real fail-then-success sequence")

		cancelCtx, cancel := context.WithCancel(ctx)
		reader = &faultedQueryClient{BucketServiceClient: client, failure: building.Err(), open: true, afterFailure: cancel}
		ids, err = workload.ReadOracleTransactions(cancelCtx, reader, "oracle", actions.ReferenceFilter("reference"))
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, ids)
		require.Equal(t, 1, reader.calls, "stop readiness polling when the caller ends")
	}, func(records []drivertest.Assertion) {
		for _, record := range records {
			require.NotEqual(t, permanentOracleError, record.Message)
		}
	})
}

func TestQueryOracleSetupError(t *testing.T) {
	drivertest.CheckEmissions(t, func() {
		ctx, client := drivertest.StartServer(t)
		err := workload.CreateQueryOracleLedger(ctx, client, "oracle",
			commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
			commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
		require.Equal(t, codes.AlreadyExists, status.Code(err))
		require.True(t, workload.HasErrorReason(err, domain.ErrReasonIndexAlreadyExists))
	}, func(records []drivertest.Assertion) {
		for _, record := range records {
			if record.Hit && record.Message == permanentOracleError {
				require.False(t, record.Condition)
				require.Equal(t, "create ledger and indexes", record.Details["operation"])
				return
			}
		}
		t.Fatal("permanent setup failure was hidden")
	})
}

func TestQueryOracleSkipsLedgerNameCollision(t *testing.T) {
	drivertest.CheckEmissions(t, func() {
		ctx, client := drivertest.StartServer(t)
		expected := seedOracle(t, ctx, client)
		err := workload.CreateQueryOracleLedger(ctx, client, "oracle", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)
		require.Equal(t, codes.AlreadyExists, status.Code(err))
		require.True(t, workload.HasErrorReason(err, domain.ErrReasonLedgerAlreadyExists))
		ids, err := workload.ReadOracleTransactions(ctx, client, "oracle", actions.ReferenceFilter("reference"))
		require.NoError(t, err)
		require.Equal(t, expected, ids, "leave the earlier invocation's ledger untouched")
	}, func(records []drivertest.Assertion) {
		for _, record := range records {
			require.NotEqual(t, permanentOracleError, record.Message)
		}
	})
}

func seedOracle(t *testing.T, ctx context.Context, client servicepb.BucketServiceClient) []uint64 {
	t.Helper()
	require.NoError(t, workload.CreateQueryOracleLedger(ctx, client, "oracle", commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE))
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("seed", &servicepb.Request{Type: &servicepb.Request_Apply{
		Apply: &servicepb.LedgerApplyRequest{Ledger: "oracle", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
			CreateTransaction: &servicepb.CreateTransactionPayload{Reference: "reference", Force: true, Postings: []*commonpb.Posting{{
				Source: "world", Destination: "account", Asset: "USD/2", Amount: commonpb.NewUint256FromUint64(1),
			}}},
		}}},
	}}))
	require.NoError(t, err)
	ids, err := workload.ReadOracleTransactions(ctx, client, "oracle", actions.ReferenceFilter("reference"))
	require.NoError(t, err)
	require.Len(t, ids, 1)
	return ids
}

// These decorators fault the response boundary of a real client/query. They
// neither implement a substitute query engine nor manufacture missing indexes.
type faultedQueryClient struct {
	servicepb.BucketServiceClient
	failure      error
	open         bool
	once         bool
	calls        int
	afterFailure func()
}

func (c *faultedQueryClient) ListTransactions(ctx context.Context, request *servicepb.ListTransactionsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[commonpb.Transaction], error) {
	c.calls++
	if c.once && c.calls > 1 {
		return c.BucketServiceClient.ListTransactions(ctx, request, opts...)
	}
	if c.open {
		if c.afterFailure != nil {
			c.afterFailure()
		}
		return nil, c.failure
	}
	stream, err := c.BucketServiceClient.ListTransactions(ctx, request, opts...)
	if err != nil {
		return nil, err
	}
	return &faultedQueryStream{ServerStreamingClient: stream, failure: c.failure}, nil
}

type faultedQueryStream struct {
	grpc.ServerStreamingClient[commonpb.Transaction]
	failure error
}

func (s *faultedQueryStream) Recv() (*commonpb.Transaction, error) {
	tx, err := s.ServerStreamingClient.Recv()
	if err == io.EOF {
		return nil, s.failure
	}
	return tx, err
}
