package health

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/service/transport"
	"github.com/formancehq/ledger-v3-poc/internal/storage/diskusage"
)

// ErrUnhealthy is returned when the cluster is not healthy (e.g. disk usage exceeded threshold).
var ErrUnhealthy = errors.New("cluster is unhealthy: disk usage exceeds threshold")

// Checker is the interface for health checking. It allows consumers (e.g. admission)
// to query the cluster health without depending on a concrete implementation.
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source healthcheck.go -destination healthcheck_generated.go -package health . Checker
type Checker interface {
	IsHealthy() bool
}

// HealthChecker periodically checks disk usage across all cluster nodes.
// It runs on every node but only performs checks when the node is the leader.
// The health state is stored and can be queried via IsHealthy().
type HealthChecker struct {
	node        *node.Node
	collector   *diskusage.Collector
	servicePool *transport.ServiceConnectionPool
	peers       []node.Peer
	logger      logging.Logger
	interval    time.Duration

	walThreshold  float64
	dataThreshold float64

	healthy atomic.Bool

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewHealthChecker creates a new HealthChecker that periodically polls disk usage
// from all cluster nodes and logs warnings when usage exceeds the per-volume thresholds.
func NewHealthChecker(
	n *node.Node,
	collector *diskusage.Collector,
	servicePool *transport.ServiceConnectionPool,
	peers []node.Peer,
	logger logging.Logger,
	interval time.Duration,
	walThreshold float64,
	dataThreshold float64,
) *HealthChecker {
	hc := &HealthChecker{
		node:          n,
		collector:     collector,
		servicePool:   servicePool,
		peers:         peers,
		logger:        logger,
		interval:      interval,
		walThreshold:  walThreshold,
		dataThreshold: dataThreshold,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
	hc.healthy.Store(true)
	return hc
}

// Start launches the background goroutine that periodically checks disk usage.
func (hc *HealthChecker) Start() {
	hc.check()

	go func() {
		defer close(hc.doneCh)
		ticker := time.NewTicker(hc.interval)
		defer ticker.Stop()

		for {
			select {
			case <-hc.stopCh:
				return
			case <-ticker.C:
				hc.check()
			}
		}
	}()
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (hc *HealthChecker) Stop() {
	close(hc.stopCh)
	<-hc.doneCh
}

// IsHealthy returns true if the last health check passed (no node exceeded the disk usage threshold).
func (hc *HealthChecker) IsHealthy() bool {
	return hc.healthy.Load()
}

// check performs the disk usage check on all nodes if this node is the leader.
// It updates the healthy state atomically.
func (hc *HealthChecker) check() {
	if !hc.node.IsLeader() {
		return
	}

	healthy := !hc.exceedsThreshold(
		hc.node.GetNodeID(),
		hc.collector.WALVolumeBytes(),
		hc.collector.WALVolumeTotalBytes(),
		hc.collector.DataVolumeBytes(),
		hc.collector.DataVolumeTotalBytes(),
	)

	// Check peers
	for _, peer := range hc.peers {
		conn := hc.servicePool.GetConnection(peer.ID)
		if conn == nil {
			hc.logger.WithFields(map[string]any{
				"node_id": peer.ID,
			}).Errorf("No connection to peer, skipping disk usage check")
			continue
		}

		client := clusterpb.NewClusterServiceClient(conn)
		resp, err := client.GetDiskUsage(context.Background(), &clusterpb.GetDiskUsageRequest{})
		if err != nil {
			hc.logger.WithFields(map[string]any{
				"node_id": peer.ID,
				"error":   err,
			}).Errorf("Failed to get disk usage from peer")
			continue
		}

		if hc.exceedsThreshold(
			peer.ID,
			resp.WalVolumeBytes,
			resp.WalVolumeTotalBytes,
			resp.DataVolumeBytes,
			resp.DataVolumeTotalBytes,
		) {
			healthy = false
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
