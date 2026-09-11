package grpc

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/formancehq/ledger/v3/internal/adapter/apierr"
	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestForwardedCursorPreservesCanceledLedgerStatus(t *testing.T) {
	t.Parallel()

	constructors := []struct {
		name string
		new  func(context.Context, ggrpc.ServerStreamingClient[commonpb.Account]) cursor.Cursor[*commonpb.Account]
	}{
		{"upstream peek", NewUpstreamPeekCursor[commonpb.Account]},
		{"generic identity", NewGRPCIdentityCursor[commonpb.Account]},
	}
	for _, constructor := range constructors {
		for _, structured := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/ledger=%t", constructor.name, structured), func(t *testing.T) {
				t.Parallel()
				const reason = "FUTURE_LEDGER_CANCELED_REASON"
				const message = "upstream operation canceled"
				metadata := map[string]string{"request": "retained-upstream-context"}
				upstream := status.New(codes.Canceled, message)
				if structured {
					var err error
					upstream, err = upstream.WithDetails(
						&errdetails.ErrorInfo{Domain: "ledger", Reason: reason, Metadata: metadata},
						&errdetails.RetryInfo{RetryDelay: durationpb.New(7 * time.Second)},
					)
					require.NoError(t, err)
				}
				client := dialCursorBoundaryServer(t, &endingBucketServer{rows: 2, endErr: upstream.Err()})
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				t.Cleanup(cancel)
				stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: "ledger"})
				require.NoError(t, err)
				cur := constructor.new(ctx, stream)
				t.Cleanup(func() { require.NoError(t, cur.Close()) })
				for i := range 2 {
					row, err := cur.Next()
					require.NoError(t, err)
					require.Equal(t, fmt.Sprintf("acc-%d", i), row.GetAddress())
				}
				_, terminal := cur.Next()
				require.Error(t, terminal)
				require.NoError(t, ctx.Err(), "the caller is still reading when the upstream status arrives")

				if !structured {
					// Genuine transport cancellation keeps its existing behavior.
					_, described := apierr.Describe(terminal)
					require.False(t, described)
					require.Equal(t, codes.Unavailable, status.Code(terminal))
					require.Contains(t, status.Convert(terminal).Message(), message)
					require.Empty(t, status.Convert(terminal).Details())

					return
				}

				want := apierr.Descriptor{Kind: domain.KindInternal, Reason: reason, Message: message, Metadata: metadata}
				// Once the actual wire failure has arrived, caller teardown
				// must not turn the already-decoded business failure into EOF.
				cancel()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				require.Same(t, terminal, normalizeStreamEnd(ctx, terminal))
				for hop := range 2 {
					descriptor, described := apierr.Describe(terminal)
					require.True(t, described, "cursor must preserve the decoded Ledger failure, hop %d", hop)
					require.Equal(t, want, descriptor)
					require.True(t, proto.Equal(upstream.Proto(), status.Convert(terminal).Proto()),
						"cursor and second hop must retain the entire upstream status, hop %d: %v", hop, terminal)
					terminal = convertToGRPCError(fmt.Errorf("follower forwarding context: %w", terminal), testLogger())
					require.True(t, proto.Equal(upstream.Proto(), status.Convert(terminal).Proto()),
						"the real server encoder must preserve all details and the safe message, hop %d", hop)
					terminal = grpcerr.FromStatusError(terminal)
				}
			})
		}
	}
}

func dialCursorBoundaryServer(t *testing.T, srv servicepb.BucketServiceServer) servicepb.BucketServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := ggrpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, srv)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-serveErr)
	})
	conn, err := ggrpc.NewClient("passthrough:///cursor-boundary",
		ggrpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		ggrpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return servicepb.NewBucketServiceClient(grpcerr.NewConn(conn))
}
