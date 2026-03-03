package health

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/diskusage"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
)

// ErrUnhealthy is returned when the cluster is not healthy (e.g. disk usage or clock skew exceeded threshold).
var ErrUnhealthy = errors.New("cluster is unhealthy")

// healthCheckCallTimeout is the per-gRPC-call timeout used when checking peer
// health. It prevents a single unreachable peer from blocking the entire
// health-check cycle (and therefore blocking shutdown).
const healthCheckCallTimeout = 5 * time.Second

// Checker is the interface for health checking. It allows consumers (e.g. admission)
// to query the cluster health without depending on a concrete implementation.
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source healthcheck.go -destination healthcheck_generated.go -package health . Checker
type Checker interface {
	IsHealthy() bool
}

// HealthChecker periodically checks disk usage and clock skew across all cluster nodes.
// It runs on every node but only performs checks when the node is the leader.
// The health state is stored and can be queried via IsHealthy().
type HealthChecker struct {
	node        *node.Node
	collector   *diskusage.Collector
	servicePool *transport.ConnectionPool
	logger      logging.Logger
	interval    time.Duration

	walThreshold       float64
	dataThreshold      float64
	clockSkewThreshold time.Duration

	healthy atomic.Bool

	w worker.Worker
}

// NewHealthChecker creates a new HealthChecker that periodically polls disk usage
// and clock skew from all cluster nodes and logs warnings when thresholds are exceeded.
func NewHealthChecker(
	n *node.Node,
	collector *diskusage.Collector,
	servicePool *transport.ConnectionPool,
	logger logging.Logger,
	interval time.Duration,
	walThreshold float64,
	dataThreshold float64,
	clockSkewThreshold time.Duration,
) *HealthChecker {
	hc := &HealthChecker{
		node:               n,
		collector:          collector,
		servicePool:        servicePool,
		logger:             logger,
		interval:           interval,
		walThreshold:       walThreshold,
		dataThreshold:      dataThreshold,
		clockSkewThreshold: clockSkewThreshold,
		w:                  worker.New(),
	}
	hc.healthy.Store(true)
	return hc
}

// Start launches the background goroutine that periodically checks disk usage.
func (hc *HealthChecker) Start() {
	hc.check(make(chan struct{})) // initial check with no-op stop
	hc.w.Run(func(stop <-chan struct{}) {
		worker.RunTicker(stop, hc.interval, func() {
			hc.check(stop)
		})
	})
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (hc *HealthChecker) Stop() {
	hc.w.Stop()
}

// IsHealthy returns true if the last health check passed (no node exceeded the disk usage threshold).
func (hc *HealthChecker) IsHealthy() bool {
	return hc.healthy.Load()
}

// check performs the disk usage check on all nodes if this node is the leader.
// It updates the healthy state atomically.
//
// stop is the worker's stop channel; it is used to derive a cancellable context
// so that in-flight gRPC calls are interrupted promptly during shutdown.
func (hc *HealthChecker) check(stop <-chan struct{}) {
	if !hc.node.IsLeader() {
		return
	}

	// Create a base context that cancels on shutdown. Each gRPC call gets
	// its own child context with a per-call timeout (healthCheckCallTimeout).
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()
	go func() {
		select {
		case <-stop:
			baseCancel()
		case <-baseCtx.Done():
		}
	}()

	healthy := !hc.exceedsThreshold(
		hc.node.GetNodeID(),
		hc.collector.WALVolumeBytes(),
		hc.collector.WALVolumeTotalBytes(),
		hc.collector.DataVolumeBytes(),
		hc.collector.DataVolumeTotalBytes(),
	)

	// Check peers dynamically from the service pool
	for _, peerID := range hc.servicePool.PeerIDs() {
		// Abort early if shutting down.
		select {
		case <-baseCtx.Done():
			return
		default:
		}

		conn := hc.servicePool.GetConnection(peerID)
		if conn == nil {
			continue
		}

		client := clusterpb.NewClusterServiceClient(conn)

		// Check disk usage
		callCtx, callCancel := context.WithTimeout(baseCtx, healthCheckCallTimeout)
		resp, err := client.GetDiskUsage(callCtx, &clusterpb.GetDiskUsageRequest{})
		callCancel()
		if err != nil {
			hc.logger.WithFields(map[string]any{
				"node_id": peerID,
				"error":   err,
			}).Errorf("Failed to get disk usage from peer")
		} else if hc.exceedsThreshold(
			peerID,
			resp.WalVolumeBytes,
			resp.WalVolumeTotalBytes,
			resp.DataVolumeBytes,
			resp.DataVolumeTotalBytes,
		) {
			healthy = false
		}

		// Check clock skew
		if hc.clockSkewThreshold > 0 {
			if hc.exceedsClockSkew(baseCtx, client, peerID) {
				healthy = false
			}
		}
	}

	hc.healthy.Store(healthy)
}

// exceedsThreshold checks the WAL and data volume usage for a given node,
// logs a warning if either exceeds its respective threshold, and returns true if so.
func (hc *HealthChecker) exceedsThreshold(nodeID uint64, walUsed, walTotal, dataUsed, dataTotal int64) bool {
	exceeded := false

	if walTotal > 0 {
		percent := float64(walUsed) / float64(walTotal)
		if percent >= hc.walThreshold {
			hc.logger.WithFields(map[string]any{
				"node_id": nodeID,
				"volume":  "wal",
				"used":    walUsed,
				"total":   walTotal,
				"percent": percent * 100,
			}).Errorf("Disk usage exceeds threshold (%.0f%%)", hc.walThreshold*100)
			exceeded = true
		}
	}

	if dataTotal > 0 {
		percent := float64(dataUsed) / float64(dataTotal)
		if percent >= hc.dataThreshold {
			hc.logger.WithFields(map[string]any{
				"node_id": nodeID,
				"volume":  "data",
				"used":    dataUsed,
				"total":   dataTotal,
				"percent": percent * 100,
			}).Errorf("Disk usage exceeds threshold (%.0f%%)", hc.dataThreshold*100)
			exceeded = true
		}
	}

	return exceeded
}

// exceedsClockSkew queries a peer's physical clock and returns true if the skew
// exceeds the configured threshold.
func (hc *HealthChecker) exceedsClockSkew(baseCtx context.Context, client clusterpb.ClusterServiceClient, nodeID uint64) bool {
	callCtx, callCancel := context.WithTimeout(baseCtx, healthCheckCallTimeout)
	defer callCancel()

	beforeCall := time.Now()
	resp, err := client.GetNodeTime(callCtx, &clusterpb.GetNodeTimeRequest{})
	if err != nil {
		hc.logger.WithFields(map[string]any{
			"node_id": nodeID,
			"error":   err,
		}).Errorf("Failed to get node time from peer")
		return false
	}
	afterCall := time.Now()

	// Use the midpoint of the request as the local reference time to account for network RTT
	localTime := beforeCall.Add(afterCall.Sub(beforeCall) / 2)
	remoteTime := time.UnixMicro(int64(resp.TimestampUs))

	skew := localTime.Sub(remoteTime)
	if skew < 0 {
		skew = -skew
	}

	if skew > hc.clockSkewThreshold {
		hc.logger.WithFields(map[string]any{
			"node_id": nodeID,
			"skew":    skew.String(),
		}).Errorf("Clock skew exceeds threshold (%s)", hc.clockSkewThreshold)
		return true
	}

	return false
}
