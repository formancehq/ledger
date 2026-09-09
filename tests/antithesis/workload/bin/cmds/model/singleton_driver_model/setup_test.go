package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestSetupLedgersCanRemainBlockedBeforeFirstOutcome(t *testing.T) {
	t.Parallel()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	applyEntered := make(chan struct{})
	servicepb.RegisterBucketServiceServer(server, &blockingSetupServer{applyEntered: applyEntered})
	go func() {
		_ = server.Serve(listener) // Stop terminates Serve with an expected error.
	}()
	t.Cleanup(server.Stop)

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDial()
	conn, err := grpc.DialContext(dialCtx, "bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	setupCtx, cancelSetup := context.WithCancel(context.Background())
	setupDone := make(chan bool, 1)
	go func() {
		setupDone <- setupLedgers(setupCtx, servicepb.NewBucketServiceClient(conn), []string{"model-blocked-0"}, map[string][]*commonpb.SetMetadataFieldTypeCommand{})
	}()

	requireSignal(t, applyEntered, "first setup Apply was not entered")
	select {
	case result := <-setupDone:
		t.Fatalf("setup returned %v before the blocked Apply was released", result)
	default:
	}

	cancelSetup()
	select {
	case result := <-setupDone:
		require.False(t, result)
	case <-time.After(5 * time.Second):
		t.Fatal("setup did not return after cancellation")
	}
}

func requireSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal(message)
	}
}

type blockingSetupServer struct {
	servicepb.UnimplementedBucketServiceServer
	applyEntered chan struct{}
}

func (s *blockingSetupServer) Apply(ctx context.Context, _ *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	close(s.applyEntered)
	<-ctx.Done()
	return nil, ctx.Err()
}
