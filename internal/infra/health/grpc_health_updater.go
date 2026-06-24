package health

import (
	"time"

	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
)

const grpcHealthUpdateInterval = 5 * time.Second

// GRPCHealthUpdater periodically evaluates cluster readiness and updates the
// gRPC health.Server status accordingly. This ensures that gRPC health checks
// (used by k8s gRPC probes or grpc-health-probe) reflect actual readiness
// rather than always reporting SERVING.
type GRPCHealthUpdater struct {
	node         nodeState
	healthServer *health.Server
	interval     time.Duration
	w            worker.Worker
}

// NewGRPCHealthUpdater creates a new updater that will poll readiness at the
// given interval and set the gRPC serving status on healthServer.
func NewGRPCHealthUpdater(n *node.Node, healthServer *health.Server) *GRPCHealthUpdater {
	return &GRPCHealthUpdater{
		node:         n,
		healthServer: healthServer,
		interval:     grpcHealthUpdateInterval,
		w:            worker.New(),
	}
}

// Start launches the background goroutine that periodically updates gRPC health status.
func (u *GRPCHealthUpdater) Start() {
	u.update() // initial evaluation
	u.w.Run(func(stop <-chan struct{}) {
		worker.RunTicker(stop, u.interval, u.update)
	})
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (u *GRPCHealthUpdater) Stop() {
	u.w.Stop()
}

func (u *GRPCHealthUpdater) update() {
	ready := u.node.IsHealthy() && u.node.GetLeader() != 0

	status := healthpb.HealthCheckResponse_NOT_SERVING
	if ready {
		status = healthpb.HealthCheckResponse_SERVING
	}

	u.healthServer.SetServingStatus("", status)
}
