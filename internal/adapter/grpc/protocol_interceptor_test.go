package grpc

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/grpc/status"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
)

// Drive the real ServiceServer chain. Generated unimplemented handlers return
// a distinct status when reached; rejected calls must never reach them.
func TestServiceServerProtocolVersion(t *testing.T) {
	t.Parallel()
	currentRevision, err := strconv.Atoi(grpcprotocol.Version)
	require.NoError(t, err)
	require.Positive(t, currentRevision)

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	srv, err := NewServiceServer(ServiceAuthPolicyRestore, internalauth.AuthConfig{}, "", 0, noopLogger{}, false, time.Second, nil, true, WithListener(listener))
	require.NoError(t, err)
	ledgerpb.RegisterBucketServiceServer(srv.GetServer(), &ledgerpb.UnimplementedBucketServiceServer{})
	ledgerpb.RegisterClusterServiceServer(srv.GetServer(), &ledgerpb.UnimplementedClusterServiceServer{})
	ledgerpb.RegisterRestoreServiceServer(srv.GetServer(), &ledgerpb.UnimplementedRestoreServiceServer{})
	healthpb.RegisterHealthServer(srv.GetServer(), health.NewServer())
	require.NoError(t, srv.Listen())
	go func() {
		_ = srv.Serve() // Stop below terminates the listener.
	}()
	t.Cleanup(func() { require.NoError(t, srv.Stop()) })

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	calls := []struct {
		name string
		call func(context.Context) error
	}{
		{"apply", func(ctx context.Context) error {
			_, err := ledgerpb.NewBucketServiceClient(conn).Apply(ctx, &ledgerpb.ApplyRequest{})

			return err
		}},
		{"list", func(ctx context.Context) error {
			stream, err := ledgerpb.NewBucketServiceClient(conn).ListLedgers(ctx, &ledgerpb.ListLedgersRequest{})
			if err != nil {
				return err
			}
			_, err = stream.Recv()

			return err
		}},
		{"cluster", func(ctx context.Context) error {
			_, err := ledgerpb.NewClusterServiceClient(conn).GetClusterState(ctx, &ledgerpb.GetClusterStateRequest{})

			return err
		}},
		{"restore", func(ctx context.Context) error {
			_, err := ledgerpb.NewRestoreServiceClient(conn).FinalizeRestore(ctx, &ledgerpb.FinalizeRestoreRequest{})

			return err
		}},
		{"restore-stream", func(ctx context.Context) error {
			stream, err := ledgerpb.NewRestoreServiceClient(conn).ValidateRestore(ctx, &ledgerpb.ValidateRestoreRequest{})
			if err != nil {
				return err
			}
			_, err = stream.Recv()

			return err
		}},
	}

	for _, call := range calls {
		t.Run(call.name, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name     string
				versions []string
			}{
				{"missing", nil},
				{"empty", []string{""}},
				{"older", []string{"0"}},
				{"previous", []string{strconv.Itoa(currentRevision - 1)}},
				{"newer", []string{strconv.Itoa(currentRevision + 1)}},
				{"invalid", []string{"dev"}},
				{"duplicate", []string{grpcprotocol.Version, grpcprotocol.Version}},
				{"conflicting", []string{grpcprotocol.Version, "0"}},
				{"matching", []string{grpcprotocol.Version}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					ctx = metadata.NewOutgoingContext(ctx, metadata.MD{grpcprotocol.MetadataKey: tc.versions})
					err := call.call(ctx)
					if tc.name == "matching" {
						require.Equal(t, codes.Unimplemented, status.Code(err), "matching protocol must reach the handler")

						return
					}
					require.Equal(t, codes.FailedPrecondition, status.Code(err))
					require.Contains(t, err.Error(), "incompatible Ledger gRPC protocol")
					require.Contains(t, err.Error(), grpcprotocol.MetadataKey+"="+grpcprotocol.Version)
				})
			}
		})
	}

	t.Run("diagnostics-without-version", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
		require.NoError(t, err)
		_, err = ledgerpb.NewBucketServiceClient(conn).Discovery(ctx, &ledgerpb.DiscoveryRequest{})
		require.Equal(t, codes.Unimplemented, status.Code(err), "Discovery must reach its handler without version metadata")
		reflection, err := reflectionpb.NewServerReflectionClient(conn).ServerReflectionInfo(ctx)
		require.NoError(t, err)
		require.NoError(t, reflection.Send(&reflectionpb.ServerReflectionRequest{
			MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{ListServices: ""},
		}))
		services, err := reflection.Recv()
		require.NoError(t, err)
		require.NotEmpty(t, services.GetListServicesResponse().GetService())
	})
}
