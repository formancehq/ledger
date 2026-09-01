package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// endingBucketServer streams `rows` accounts and then ends the stream with
// `endErr` — nil for a clean completion, a Canceled status for a handler
// dying mid-stream (the wire shape of a forwarded read whose serving node
// is torn down while the follower still waits for rows).
type endingBucketServer struct {
	servicepb.UnimplementedBucketServiceServer

	rows   int
	endErr error
}

func (s *endingBucketServer) ListAccounts(_ *servicepb.ListAccountsRequest, stream ggrpc.ServerStreamingServer[commonpb.Account]) error {
	for i := range s.rows {
		if err := stream.Send(&commonpb.Account{Address: fmt.Sprintf("acc-%d", i)}); err != nil {
			return err
		}
	}

	return s.endErr
}

// dialBucketServer serves srv over an in-process bufconn listener and
// returns a connected client.
func dialBucketServer(t *testing.T, srv servicepb.BucketServiceServer) servicepb.BucketServiceClient {
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
	if err != nil {
		t.Fatalf("dialing bufconn: %v", err)
	}

	t.Cleanup(func() { _ = conn.Close() })

	return servicepb.NewBucketServiceClient(conn)
}

// drainPeekCursor consumes the cursor to its terminal error, the way
// sendPagedToStream does, returning the items and that error.
func drainPeekCursor(t *testing.T, ctx context.Context, stream ggrpc.ServerStreamingClient[commonpb.Account]) ([]*commonpb.Account, error) {
	t.Helper()

	cur := NewUpstreamPeekCursor(ctx, stream)

	var items []*commonpb.Account

	for {
		item, err := cur.Next()
		if err != nil {
			return items, err
		}

		items = append(items, item)
	}
}

// TestUpstreamPeekCursorStreamDeath exercises the cursor against a real gRPC
// stream, not a fake: the discriminator between a dead transfer and a
// consumer-side teardown lives in grpc-go's context plumbing, which a fake
// cannot reproduce (the stream-derived context is canceled on EVERY
// termination — see normalizeStreamEnd).
func TestUpstreamPeekCursorStreamDeath(t *testing.T) {
	t.Run("mid-stream cancellation under a live caller propagates", func(t *testing.T) {
		client := dialBucketServer(t, &endingBucketServer{rows: 2, endErr: status.Error(codes.Canceled, "serving node torn down")})

		ctx := context.Background()

		stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: "l"})
		if err != nil {
			t.Fatalf("opening stream: %v", err)
		}

		items, err := drainPeekCursor(t, ctx, stream)
		if len(items) != 2 {
			t.Fatalf("want the 2 rows sent before the death, got %d", len(items))
		}

		if errors.Is(err, io.EOF) {
			t.Fatalf("a dead transfer surfaced as clean EOF: a caller would serve the truncated page with OK status")
		}

		// The not-ours cancellation is re-coded as the peer-transport
		// transient it is: Unavailable routes it into every retry path
		// (Canceled would read as the caller's own shutdown downstream).
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("want Unavailable, got %v", err)
		}

		// The stream-derived context is already canceled here even though the
		// caller's context is live — which is why normalizeStreamEnd must
		// judge by the caller's context, never by ClientStream.Context().
		if stream.Context().Err() == nil {
			t.Fatalf("expected grpc-go to have canceled the stream-derived context on termination")
		}
	})

	t.Run("clean completion is EOF", func(t *testing.T) {
		client := dialBucketServer(t, &endingBucketServer{rows: 2})

		ctx := context.Background()

		stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: "l"})
		if err != nil {
			t.Fatalf("opening stream: %v", err)
		}

		items, err := drainPeekCursor(t, ctx, stream)
		if len(items) != 2 || !errors.Is(err, io.EOF) {
			t.Fatalf("want 2 rows then io.EOF, got %d rows, err %v", len(items), err)
		}
	})

	t.Run("caller cancellation is a clean end", func(t *testing.T) {
		client := dialBucketServer(t, &endingBucketServer{rows: 1})

		ctx, cancel := context.WithCancel(context.Background())

		stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: "l"})
		if err != nil {
			t.Fatalf("opening stream: %v", err)
		}

		cancel()

		_, err = drainPeekCursor(t, ctx, stream)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("a consumer tearing down its own read must see clean EOF, got %v", err)
		}
	})
}
