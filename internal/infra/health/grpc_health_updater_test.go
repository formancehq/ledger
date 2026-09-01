package health

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// TestGRPCHealthUpdater_ReadinessIndependentOfDiskSkew locks in the central
// EN-1322 contract: gRPC readiness is derived solely from Raft connectivity
// (IsHealthy) and leader election (GetLeader). The updater has no disk/clock
// (WriteGate) input at all — there is nothing disk/skew-related to vary here,
// which is exactly the regression this test guards.
func TestGRPCHealthUpdater_ReadinessIndependentOfDiskSkew(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		healthy     bool
		leader      uint64
		wantServing bool
	}{
		{"healthy and leader elected -> serving", true, 2, true},
		{"healthy but no leader -> not serving", true, 0, false},
		{"unhealthy with leader -> not serving", false, 2, false},
		{"unhealthy and no leader -> not serving", false, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ns := NewMocknodeState(gomock.NewController(t))
			ns.EXPECT().IsHealthy().Return(tt.healthy).AnyTimes()
			ns.EXPECT().GetLeader().Return(tt.leader).AnyTimes()

			hs := health.NewServer()
			u := &GRPCHealthUpdater{node: ns, healthServer: hs, clusterPolicyReady: func() bool { return true }}

			u.update()

			resp, err := hs.Check(context.Background(), &healthpb.HealthCheckRequest{Service: ""})
			require.NoError(t, err)

			want := healthpb.HealthCheckResponse_NOT_SERVING
			if tt.wantServing {
				want = healthpb.HealthCheckResponse_SERVING
			}
			require.Equal(t, want, resp.GetStatus())
		})
	}
}

// A healthy leader is still not write-ready until the cluster policy is
// committed at least once (EN-1827).
func TestGRPCHealthUpdater_WaitsForClusterPolicy(t *testing.T) {
	t.Parallel()

	ns := NewMocknodeState(gomock.NewController(t))
	ns.EXPECT().IsHealthy().Return(true).AnyTimes()
	ns.EXPECT().GetLeader().Return(uint64(2)).AnyTimes()

	hs := health.NewServer()
	policyReady := false
	u := &GRPCHealthUpdater{node: ns, healthServer: hs, clusterPolicyReady: func() bool { return policyReady }}

	u.update()
	resp, err := hs.Check(context.Background(), &healthpb.HealthCheckRequest{Service: ""})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_NOT_SERVING, resp.GetStatus(),
		"a healthy leader without a committed policy must not be serving")

	policyReady = true
	u.update()
	resp, err = hs.Check(context.Background(), &healthpb.HealthCheckRequest{Service: ""})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus(),
		"once the policy is committed the node becomes serving")
}
