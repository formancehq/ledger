package actions

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type unavailableHealthServer struct {
	healthpb.UnimplementedHealthServer

	attempts atomic.Int32
}

func (s *unavailableHealthServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	s.attempts.Add(1)

	return nil, status.Error(codes.Unavailable, "no leader")
}

func TestGRPCRetryPolicyAttempts(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	health := &unavailableHealthServer{}
	healthpb.RegisterHealthServer(server, health)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(GRPCRetryPolicy),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Check() code = %s, want UNAVAILABLE (error: %v)", status.Code(err), err)
	}
	if got := health.attempts.Load(); got != 5 {
		t.Fatalf("server saw %d attempts, policy declares 5", got)
	}
}
