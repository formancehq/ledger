package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type unannotatedBucketService interface {
	Unannotated(context.Context, any) (any, error)
}

type unannotatedBucketServiceImpl struct{}

func (unannotatedBucketServiceImpl) Unannotated(context.Context, any) (any, error) {
	return nil, nil
}

func TestPublicRPCPolicyValidationRejectsUnannotatedMethodBeforeListen(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	srv, err := NewServiceServer(ServiceAuthPolicyPublic, "", 0, noopLogger{}, false, time.Second, nil, true, WithListener(listener))
	require.NoError(t, err)

	srv.GetServer().RegisterService(&ggrpc.ServiceDesc{
		ServiceName: "ledger.BucketService",
		HandlerType: (*unannotatedBucketService)(nil),
		Methods: []ggrpc.MethodDesc{{
			MethodName: "Unannotated",
			Handler: func(any, context.Context, func(any) error, ggrpc.UnaryServerInterceptor) (any, error) {
				return nil, nil
			},
		}},
	}, unannotatedBucketServiceImpl{})

	err = srv.Listen()
	require.ErrorContains(t, err, "/ledger.BucketService/Unannotated")
	require.False(t, srv.listened)
	require.Nil(t, srv.listener)
}

func TestPublicRPCPolicyValidationAcceptsGeneratedServiceDescriptors(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	srv, err := NewServiceServer(ServiceAuthPolicyPublic, "", 0, noopLogger{}, false, time.Second, nil, true, WithListener(listener))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, srv.Stop()) })

	servicepb.RegisterBucketServiceServer(srv.GetServer(), &servicepb.UnimplementedBucketServiceServer{})
	clusterpb.RegisterClusterServiceServer(srv.GetServer(), &clusterpb.UnimplementedClusterServiceServer{})
	healthpb.RegisterHealthServer(srv.GetServer(), health.NewServer())

	require.NoError(t, srv.Listen())
}

func TestPublicRPCPolicyValidationRejectsUnregisteredPolicyBeforeListen(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	srv, err := NewServiceServer(ServiceAuthPolicyPublic, "", 0, noopLogger{}, false, time.Second, nil, true, WithListener(listener))
	require.NoError(t, err)
	servicepb.RegisterBucketServiceServer(srv.GetServer(), &servicepb.UnimplementedBucketServiceServer{})

	err = srv.Listen()
	require.ErrorContains(t, err, "authentication policy has no registered RPC")
	require.False(t, srv.listened)
	require.Nil(t, srv.listener)
}

func TestPublicRPCPolicyValidationUsesExactInfrastructureAllowlist(t *testing.T) {
	t.Parallel()

	err := validatePublicRPCPolicies(map[string]ggrpc.ServiceInfo{
		"grpc.health.v1.Health": {
			Methods: []ggrpc.MethodInfo{{Name: "Unknown"}},
		},
	})

	require.ErrorContains(t, err, "/grpc.health.v1.Health/Unknown")
}
