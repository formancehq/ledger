package cmdutil

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
)

// Exercise the production constructors, not ClientOption in isolation: both
// command families must advertise their protocol on unary and streaming RPCs.
func TestClientsAdvertiseProtocol(t *testing.T) {
	t.Parallel()

	constructors := map[string]func(*cobra.Command) (*grpc.ClientConn, error){
		"bucket": func(cmd *cobra.Command) (*grpc.ClientConn, error) {
			_, conn, err := GetClient(cmd)

			return conn, err
		},
		"cluster": func(cmd *cobra.Command) (*grpc.ClientConn, error) {
			_, conn, err := GetClusterClient(cmd)

			return conn, err
		},
	}

	for name, connect := range constructors {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			listener, err := net.Listen("tcp4", "127.0.0.1:0")
			require.NoError(t, err)
			server := grpc.NewServer(
				grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
					md, _ := metadata.FromIncomingContext(ctx)
					if err := grpc.SendHeader(ctx, protocolTestHeaders(md)); err != nil {
						return nil, err
					}

					return handler(ctx, req)
				}),
				grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, _ *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
					md, _ := metadata.FromIncomingContext(stream.Context())
					if err := stream.SendHeader(protocolTestHeaders(md)); err != nil {
						return err
					}

					return handler(srv, stream)
				}),
			)
			healthpb.RegisterHealthServer(server, health.NewServer())
			go func() { _ = server.Serve(listener) }() // Stop closes the owned listener.
			t.Cleanup(server.Stop)

			cmd := newTLSFlagCommand()
			cmd.Flags().String("server", listener.Addr().String(), "")
			cmd.Flags().String("auth-token", "test-token", "")
			cmd.Flags().String("consistency", "stale", "")
			require.NoError(t, cmd.Flags().Set("insecure", "true"))
			cmd.SetContext(context.Background())

			conn, err := connect(cmd)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			ctx, cancel := GetContext(cmd)
			defer cancel()
			ctx, cancelDeadline := context.WithTimeout(ctx, 5*time.Second)
			defer cancelDeadline()

			client := healthpb.NewHealthClient(conn)
			var unaryHeaders metadata.MD
			_, err = client.Check(ctx, &healthpb.HealthCheckRequest{}, grpc.Header(&unaryHeaders))
			require.NoError(t, err)

			stream, err := client.Watch(ctx, &healthpb.HealthCheckRequest{})
			require.NoError(t, err)
			_, err = stream.Recv()
			require.NoError(t, err)
			streamHeaders, err := stream.Header()
			require.NoError(t, err)

			for _, headers := range []metadata.MD{unaryHeaders, streamHeaders} {
				require.Equal(t, []string{grpcprotocol.Version}, headers.Get(grpcprotocol.MetadataKey))
				require.Equal(t, []string{"Bearer test-token"}, headers.Get("authorization"))
				require.Equal(t, []string{"stale"}, headers.Get("x-consistency"))
			}
		})
	}
}

func protocolTestHeaders(md metadata.MD) metadata.MD {
	return metadata.MD{
		grpcprotocol.MetadataKey: md.Get(grpcprotocol.MetadataKey),
		"authorization":          md.Get("authorization"),
		"x-consistency":          md.Get("x-consistency"),
	}
}
