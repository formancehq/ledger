package testserver_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
)

func TestPublicClientSharesServerDescriptorRegistry(t *testing.T) {
	lease := testserver.AllocateNodeLease()
	config := testserver.TestNodeConfig{
		NodeID: 1, ClusterID: "public-client-test", Ports: lease.Ports(),
		WalDir: t.TempDir(), DataDir: t.TempDir(), Output: t.Output(),
	}
	instruments := append(testserver.DefaultTestInstruments(config), testserver.WithBootstrap())
	server := lease.NewService(cmdserver.NewRunCommandWithBindings,
		testservice.WithInstruments(instruments...))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})

	registered, err := protoregistry.GlobalFiles.FindFileByPath("signature.proto")
	require.NoError(t, err)
	require.Equal(t, ledgerpb.File_signature_proto, registered)
	require.Equal(t, grpcprotocol.Version, ledgerpb.ProtocolVersion)

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", lease.Ports().GRPC()),
		grpc.WithTransportCredentials(insecure.NewCredentials()), ledgerpb.ClientOption())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := ledgerpb.NewBucketServiceClient(conn)
	discovery, err := client.Discovery(ctx, &ledgerpb.DiscoveryRequest{})
	require.NoError(t, err)
	require.Equal(t, grpcprotocol.Version, discovery.GetServerInfo().GetProtocolVersion())

	require.Eventually(t, func() bool {
		stream, err := client.ListLedgers(ctx, &ledgerpb.ListLedgersRequest{})
		if err != nil {
			return false
		}
		_, err = stream.Recv()

		return errors.Is(err, io.EOF)
	}, 10*time.Second, 100*time.Millisecond)
}
