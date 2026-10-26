package actions

import (
	"context"
	"encoding/json"
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

func TestGRPCRetryPolicyCoversLeaderElection(t *testing.T) {
	t.Parallel()

	var config struct {
		MethodConfig []struct {
			RetryPolicy struct {
				MaxAttempts       int     `json:"MaxAttempts"`
				InitialBackoff    string  `json:"InitialBackoff"`
				MaxBackoff        string  `json:"MaxBackoff"`
				BackoffMultiplier float64 `json:"BackoffMultiplier"`
			} `json:"retryPolicy"`
		} `json:"methodConfig"`
	}
	if err := json.Unmarshal([]byte(GRPCRetryPolicy), &config); err != nil {
		t.Fatal(err)
	}
	if len(config.MethodConfig) != 1 {
		t.Fatalf("method configs = %d, want 1", len(config.MethodConfig))
	}
	policy := config.MethodConfig[0].RetryPolicy
	initial, err := time.ParseDuration(policy.InitialBackoff)
	if err != nil {
		t.Fatal(err)
	}
	maximum, err := time.ParseDuration(policy.MaxBackoff)
	if err != nil {
		t.Fatal(err)
	}
	if policy.MaxAttempts != 5 || initial <= 0 || maximum < initial || policy.BackoffMultiplier < 1 {
		t.Fatalf("invalid retry budget: %+v", policy)
	}

	var budget time.Duration
	backoff := initial
	for range policy.MaxAttempts - 1 {
		budget += min(backoff, maximum)
		backoff = time.Duration(float64(backoff) * policy.BackoffMultiplier)
	}
	// grpc-go adds up to 20% jitter. Even at the low end, the policy must
	// outlast a two-second election at the default Raft settings.
	if minimum := budget * 8 / 10; minimum < 2*time.Second {
		t.Fatalf("minimum backoff budget = %s, want at least 2s", minimum)
	}
}
