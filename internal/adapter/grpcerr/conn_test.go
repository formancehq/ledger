package grpcerr

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// rejectingServer answers with a business error, the way a leader rejects a
// forwarded request. GetTransaction covers the unary path (Invoke);
// ListLedgers covers the streaming path, where the error reaches the client at
// Recv() rather than from the method call — the case per-method conversion
// cannot reach, because the method has already returned nil by then.
type rejectingServer struct {
	servicepb.UnimplementedBucketServiceServer

	err error

	// rowsBeforeError, when > 0, sends that many items before failing, so the
	// stream error genuinely arrives on a later Recv rather than the first.
	rowsBeforeError int
}

func (s *rejectingServer) GetTransaction(
	context.Context, *servicepb.GetTransactionRequest,
) (*servicepb.GetTransactionResponse, error) {
	return nil, s.err
}

func (s *rejectingServer) ListLedgers(
	_ *servicepb.ListLedgersRequest,
	stream ggrpc.ServerStreamingServer[commonpb.LedgerInfo],
) error {
	for range s.rowsBeforeError {
		if err := stream.Send(&commonpb.LedgerInfo{Name: "some-ledger"}); err != nil {
			return err
		}
	}

	return s.err
}

// dialWrapped serves srv over bufconn and returns a client whose connection is
// decorated, so the test exercises the real generated client against the real
// wire — not a hand-built status value.
func dialWrapped(t *testing.T, srv servicepb.BucketServiceServer) servicepb.BucketServiceClient {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	server := ggrpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, srv)

	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)

	conn, err := ggrpc.NewClient("passthrough:///bufconn",
		ggrpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		ggrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return servicepb.NewBucketServiceClient(NewConn(conn))
}

// businessStatus builds the status a leader sends for a business rejection.
func businessStatus(t *testing.T, code codes.Code, message, reason string) error {
	t.Helper()

	st := status.New(code, message)
	detailed, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "ledger",
	})
	require.NoError(t, err)

	return detailed.Err()
}

func TestConn_UnaryErrorIsReconstructed(t *testing.T) {
	t.Parallel()

	client := dialWrapped(t, &rejectingServer{
		err: businessStatus(t, codes.FailedPrecondition,
			"metadata field not declared in schema: TARGET_TYPE_ACCOUNT/type",
			domain.ErrReasonMetadataFieldNotInSchema),
	})

	_, err := client.GetTransaction(context.Background(), &servicepb.GetTransactionRequest{
		Ledger:        "test",
		TransactionId: 1,
	})
	require.Error(t, err)

	d, ok := errors.AsType[domain.Describable](err)
	require.True(t, ok, "a forwarded business error must arrive as a Describable")
	require.Equal(t, domain.ErrReasonMetadataFieldNotInSchema, d.Reason())
	require.Equal(t, domain.KindPrecondition, domain.Kind(d))

	require.Equal(t, codes.FailedPrecondition, status.Code(err), "the status code must survive")
}

// TestConn_StreamErrorIsReconstructed is the case the ticket's per-method
// proposal would have missed: ListLedgers returns (stream, nil), and the
// leader's rejection only surfaces on a later Recv.
func TestConn_StreamErrorIsReconstructed(t *testing.T) {
	t.Parallel()

	client := dialWrapped(t, &rejectingServer{
		rowsBeforeError: 2,
		err:             businessStatus(t, codes.FailedPrecondition, "ledger deleted: foo", domain.ErrReasonLedgerDeleted),
	})

	stream, err := client.ListLedgers(context.Background(), &servicepb.ListLedgersRequest{})
	require.NoError(t, err, "the rejection is not reported here — that is the point")

	var received int

	var recvErr error

	for {
		if _, recvErr = stream.Recv(); recvErr != nil {
			break
		}

		received++
	}

	require.Equal(t, 2, received, "the rows before the error must still arrive")
	require.NotErrorIs(t, recvErr, io.EOF)

	d, ok := errors.AsType[domain.Describable](recvErr)
	require.True(t, ok, "a business error arriving at Recv must also be reconstructed")
	require.Equal(t, domain.ErrReasonLedgerDeleted, d.Reason())
	require.Equal(t, domain.KindConflict, domain.Kind(d), "409, not the 400 the wire code alone implies")
}

// TestConn_StreamEndStillEOF pins the pagination contract: a clean stream end
// must remain io.EOF through the decorator, or every cursor stops terminating.
func TestConn_StreamEndStillEOF(t *testing.T) {
	t.Parallel()

	client := dialWrapped(t, &rejectingServer{rowsBeforeError: 3, err: nil})

	stream, err := client.ListLedgers(context.Background(), &servicepb.ListLedgersRequest{})
	require.NoError(t, err)

	var received int

	for {
		if _, err = stream.Recv(); err != nil {
			break
		}

		received++
	}

	require.Equal(t, 3, received)
	require.ErrorIs(t, err, io.EOF, "a clean stream end must stay io.EOF")
}

// TestConn_StreamCanceledStaysCanceled pins the other half of that contract:
// internal/adapter/grpc/cursor.go normalises a codes.Canceled Recv error into
// io.EOF to close a page, which only works while the code is visible.
func TestConn_StreamCanceledStaysCanceled(t *testing.T) {
	t.Parallel()

	client := dialWrapped(t, &rejectingServer{
		rowsBeforeError: 1,
		err:             status.Error(codes.Canceled, "serving node torn down"),
	})

	stream, err := client.ListLedgers(context.Background(), &servicepb.ListLedgersRequest{})
	require.NoError(t, err)

	for {
		if _, err = stream.Recv(); err != nil {
			break
		}
	}

	require.Equal(t, codes.Canceled, status.Code(err))
}

// TestConn_BareNotFoundIsReconstructed covers the commonpb.NewNotFoundError
// sites, which send no ErrorInfo.
func TestConn_BareNotFoundIsReconstructed(t *testing.T) {
	t.Parallel()

	client := dialWrapped(t, &rejectingServer{err: status.Error(codes.NotFound, "ledger foo not found")})

	_, err := client.GetTransaction(context.Background(), &servicepb.GetTransactionRequest{
		Ledger:        "foo",
		TransactionId: 1,
	})
	require.Error(t, err)

	_, ok := errors.AsType[*commonpb.NotFoundError](err)
	require.True(t, ok)
}

// TestConn_SuccessPathUnaffected: the decorator must not disturb a call that
// succeeds.
func TestConn_SuccessPathUnaffected(t *testing.T) {
	t.Parallel()

	client := dialWrapped(t, &rejectingServer{rowsBeforeError: 1, err: nil})

	stream, err := client.ListLedgers(context.Background(), &servicepb.ListLedgersRequest{})
	require.NoError(t, err)

	info, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, "some-ledger", info.GetName())
}
