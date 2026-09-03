package health

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/monitoring/diskusage"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

// healthCheckCallTimeout is the per-gRPC-call timeout used when checking peer
// health. It prevents a single unreachable peer from blocking the entire
// health-check cycle (and therefore blocking shutdown).
const healthCheckCallTimeout = 5 * time.Second

// WriteGate reports whether writes are currently permitted. A nil return means
// writes are allowed; a non-nil return is a domain.Describable error
// (ErrWritesBlockedDiskFull or ErrWritesBlockedClockSkew) identifying why.
//
//go:generate mockgen -write_source_comment=false -write_package_comment=false -source healthcheck.go -destination healthcheck_generated.go -package health . WriteGate
type WriteGate interface {
	CheckWritesAllowed() error
}

// gateState is the write-gate verdict published as one atomic value so
// CheckWritesAllowed observes both block reasons consistently — no torn read
// between separate stores during a block-reason transition (e.g. disk recovers
// below resume while clock skew is newly detected in the same cycle).
type gateState struct {
	diskBlocked     bool
	skewBlocked     bool
	leadershipEpoch uint64
}

// HealthChecker periodically samples disk usage and clock skew across cluster
// nodes and maintains the write-gate state (gateState). It runs on every node
// but only evaluates when this node is the leader: the leader owns the
// cluster-wide write verdict via its own disk plus peer polls. Readiness does
// NOT depend on this state (see grpc_health_updater / http).
type HealthChecker struct {
	node        nodeState
	collector   *diskusage.Collector
	servicePool *transport.ConnectionPool
	logger      logging.Logger
	interval    time.Duration

	thresholds         Thresholds
	clockSkewThreshold time.Duration

	gate atomic.Pointer[gateState]
	// leadershipEpoch invalidates node-local verdicts synchronously with Raft
	// leadership changes. A verdict from an older term can still finish its poll,
	// but CheckWritesAllowed will reject it because its epoch no longer matches.
	leadershipEpoch atomic.Uint64
	pollFailures    metric.Int64Counter
	checkNow        chan struct{}

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
	thresholds Thresholds,
	clockSkewThreshold time.Duration,
	meter metric.Meter,
) *HealthChecker {
	hc := &HealthChecker{
		node:               n,
		collector:          collector,
		servicePool:        servicePool,
		logger:             logger,
		interval:           interval,
		thresholds:         thresholds,
		clockSkewThreshold: clockSkewThreshold,
		checkNow:           make(chan struct{}, 1),
		w:                  worker.New(),
	}

	// Callers always pass a real or noop meter (never nil). The constructor
	// error is ignored per the codebase idiom: modern otel returns a usable
	// (non-nil) instrument even on error.
	hc.pollFailures, _ = meter.Int64Counter(
		"health.disk.poll.failures",
		metric.WithDescription("Count of failed GetDiskUsage polls to peers (failures cannot clear an existing disk write gate)"),
	)

	// The gate pointer defaults to nil, which CheckWritesAllowed treats as an
	// unknown disk verdict. Writes fail closed until the leader publishes its
	// first complete, fresh cluster-wide evaluation.
	return hc
}

// Start launches the background goroutine that periodically checks disk usage.
func (hc *HealthChecker) Start() {
	hc.check(make(chan struct{})) // initial check with no-op stop
	hc.w.Run(func(stop <-chan struct{}) {
		ticker := time.NewTicker(hc.interval)
		defer ticker.Stop()

		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
			case <-hc.checkNow:
			}
			hc.check(stop)
		}
	})
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (hc *HealthChecker) Stop() {
	hc.w.Stop()
}

// CheckWritesAllowed implements WriteGate. It Loads the gate once so both block
// reasons are read from a single consistent snapshot. A nil or previous-term
// gate is unknown and fails closed until the leader has collected one complete
// set of fresh samples for the current leadership epoch.
func (hc *HealthChecker) CheckWritesAllowed() error {
	s := hc.gate.Load()
	if s == nil || s.leadershipEpoch != hc.leadershipEpoch.Load() {
		return domain.ErrWritesBlockedDiskFull
	}

	return writeGateErrorForState(s.diskBlocked, s.skewBlocked)
}

// OnLeadershipChange invalidates the node-local verdict before any admission
// can run under the new Raft leadership term. It is intentionally cheap and is
// called synchronously from the node observer.
func (hc *HealthChecker) OnLeadershipChange(bool) {
	hc.leadershipEpoch.Add(1)
	hc.gate.Store(nil)
	// The LeadershipChangeEvent is emitted before node.IsLeader observes the
	// new SoftState. LeaderReady triggers the first poll after that publication.
}

// OnLeaderReady schedules the first current-epoch check after Raft has
// published leadership and caught the FSM up. The work remains serialized on
// the health worker instead of blocking the node observer thread.
func (hc *HealthChecker) OnLeaderReady() {
	if hc.checkNow == nil {
		return
	}
	// Coalesce repeated notifications while a check is already pending.
	select {
	case hc.checkNow <- struct{}{}:
	default:
	}
}

// nodeUsageReport holds the disk usage data for a single node, used for info logging.
type nodeUsageReport struct {
	nodeID      uint64
	walUsed     uint64
	walTotal    uint64
	walPercent  float64
	walValid    bool
	walAge      time.Duration
	walError    string
	dataUsed    uint64
	dataTotal   uint64
	dataPercent float64
	dataValid   bool
	dataAge     time.Duration
	dataError   string
	fetchErr    error
}

// check samples disk usage and clock skew across all nodes if this node is the
// leader, then publishes the write-gate state (gateState) as a single atomic swap.
//
// stop is the worker's stop channel; it is used to derive a cancellable context
// so that in-flight gRPC calls are interrupted promptly during shutdown.
func (hc *HealthChecker) check(stop <-chan struct{}) {
	if !hc.node.IsLeader() {
		// OnLeadershipChange invalidates the verdict synchronously. Keep the
		// follower path side-effect free so a slow old-term poll cannot establish
		// a current-term verdict.
		return
	}
	leadershipEpoch := hc.leadershipEpoch.Load()

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

	var reports []nodeUsageReport
	var samples []VolumeSample
	skewExceeded := false

	now := time.Now()
	localWAL := hc.collector.WALVolume.Load()
	localData := hc.collector.DataVolume.Load()
	localWalUsed := uint64(localWAL.UsedBytes)
	localWalTotal := uint64(localWAL.TotalBytes)
	localDataUsed := uint64(localData.UsedBytes)
	localDataTotal := uint64(localData.TotalBytes)
	localWalValid := localWAL.Usable(now)
	localDataValid := localData.Usable(now)

	localNodeID := hc.node.GetNodeID()
	hc.logIfAtBlock(localNodeID, localWalUsed, localWalTotal, localWalValid, localDataUsed, localDataTotal, localDataValid)

	samples = append(samples, VolumeSample{
		WALFraction:  safeFraction(localWalUsed, localWalTotal),
		WALValid:     localWalValid,
		DataFraction: safeFraction(localDataUsed, localDataTotal),
		DataValid:    localDataValid,
	})

	reports = append(reports, nodeUsageReport{
		nodeID:      localNodeID,
		walUsed:     localWalUsed,
		walTotal:    localWalTotal,
		walPercent:  safePercent(localWalUsed, localWalTotal),
		walValid:    localWalValid,
		walAge:      sampleAge(now, localWAL.ObservedAt),
		walError:    localWAL.Error,
		dataUsed:    localDataUsed,
		dataTotal:   localDataTotal,
		dataPercent: safePercent(localDataUsed, localDataTotal),
		dataValid:   localDataValid,
		dataAge:     sampleAge(now, localData.ObservedAt),
		dataError:   localData.Error,
	})

	// Raft membership is authoritative: a committed peer that failed to enter
	// the service pool is still an unreachable sample and cannot clear an
	// existing disk write gate.
	for _, peerID := range hc.node.MemberIDs() {
		if peerID == localNodeID {
			continue
		}

		// Abort early if shutting down.
		select {
		case <-baseCtx.Done():
			return
		default:
		}

		conn := hc.servicePool.GetConnection(peerID)
		if conn == nil {
			err := errors.New("peer connection is unavailable")
			hc.logger.WithFields(map[string]any{
				"node_id": peerID,
				"error":   err,
			}).Errorf("Failed to get disk usage from peer")
			hc.pollFailures.Add(context.Background(), 1, metric.WithAttributes(attribute.Int64("node_id", int64(peerID))))
			reports = append(reports, nodeUsageReport{nodeID: peerID, fetchErr: err})
			samples = append(samples, VolumeSample{})

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

			hc.pollFailures.Add(context.Background(), 1, metric.WithAttributes(attribute.Int64("node_id", int64(peerID))))

			reports = append(reports, nodeUsageReport{
				nodeID:   peerID,
				fetchErr: err,
			})
			// A missing peer sample cannot establish a new disk-full state, but it
			// must prevent an existing state from being cleared without evidence.
			samples = append(samples, VolumeSample{})
		} else {
			walVolume := resp.GetWalVolume()
			dataVolume := resp.GetDataVolume()
			walUsed := walVolume.GetUsedBytes()
			walTotal := walVolume.GetTotalBytes()
			dataUsed := dataVolume.GetUsedBytes()
			dataTotal := dataVolume.GetTotalBytes()
			walValid, walError := remoteVolumeValidity(walVolume)
			dataValid, dataError := remoteVolumeValidity(dataVolume)

			hc.logIfAtBlock(peerID, walUsed, walTotal, walValid, dataUsed, dataTotal, dataValid)

			samples = append(samples, VolumeSample{
				WALFraction:  safeFraction(walUsed, walTotal),
				WALValid:     walValid,
				DataFraction: safeFraction(dataUsed, dataTotal),
				DataValid:    dataValid,
			})

			reports = append(reports, nodeUsageReport{
				nodeID:      peerID,
				walUsed:     walUsed,
				walTotal:    walTotal,
				walPercent:  safePercent(walUsed, walTotal),
				walValid:    walValid,
				walAge:      time.Duration(walVolume.GetSampleAgeMs()) * time.Millisecond,
				walError:    walError,
				dataUsed:    dataUsed,
				dataTotal:   dataTotal,
				dataPercent: safePercent(dataUsed, dataTotal),
				dataValid:   dataValid,
				dataAge:     time.Duration(dataVolume.GetSampleAgeMs()) * time.Millisecond,
				dataError:   dataError,
			})
		}

		// Check clock skew
		if hc.clockSkewThreshold > 0 {
			if hc.exceedsClockSkew(baseCtx, client, peerID) {
				skewExceeded = true
			}
		}
	}

	// Publish both block reasons as a single gateState swap so CheckWritesAllowed
	// can never observe a torn intermediate state (e.g. disk cleared but skew not
	// yet set) during a block-reason transition. The disk hysteresis read of the
	// previous state (prev.diskBlocked below) is race-free without a lock because
	// check() runs only on the single worker goroutine — it is the sole writer of
	// the gate. CheckWritesAllowed only ever Loads it.
	var (
		prevDiskBlocked   bool
		hasCurrentVerdict bool
	)
	if prev := hc.gate.Load(); prev != nil && prev.leadershipEpoch == leadershipEpoch {
		prevDiskBlocked = prev.diskBlocked
		hasCurrentVerdict = true
	}
	if !hasCurrentVerdict && !allSamplesValid(samples) {
		// A new leader has no inherited hysteresis state. Do not publish an open
		// gate until every committed member reports both volumes with current
		// evidence; gate==nil remains fail-closed for this leadership epoch.
		hc.logDiskUsageSummary(reports)

		return
	}

	diskBlocked := hc.thresholds.NextDiskBlocked(prevDiskBlocked, samples)
	if !hasCurrentVerdict {
		// With a complete first sample set there is no prior hysteresis verdict to
		// preserve. Apply only the block threshold, so values inside the hysteresis
		// band do not create a synthetic block on leadership acquisition.
		diskBlocked = hc.thresholds.anyAtBlock(samples)
	}

	hc.gate.Store(&gateState{
		diskBlocked:     diskBlocked,
		skewBlocked:     skewExceeded,
		leadershipEpoch: leadershipEpoch,
	})

	hc.logDiskUsageSummary(reports)
}

// logDiskUsageSummary logs an info-level message summarizing disk usage across all nodes.
func (hc *HealthChecker) logDiskUsageSummary(reports []nodeUsageReport) {
	for _, r := range reports {
		fields := map[string]any{
			"node_id": r.nodeID,
		}

		if r.fetchErr != nil {
			fields["error"] = r.fetchErr
			hc.logger.WithFields(fields).Infof("Disk usage check: unreachable")

			continue
		}

		fields["wal_used"] = r.walUsed
		fields["wal_total"] = r.walTotal
		fields["wal_percent"] = r.walPercent
		fields["wal_valid"] = r.walValid
		fields["wal_sample_age"] = r.walAge.String()
		if r.walError != "" {
			fields["wal_error"] = r.walError
		}
		fields["data_used"] = r.dataUsed
		fields["data_total"] = r.dataTotal
		fields["data_percent"] = r.dataPercent
		fields["data_valid"] = r.dataValid
		fields["data_sample_age"] = r.dataAge.String()
		if r.dataError != "" {
			fields["data_error"] = r.dataError
		}
		hc.logger.WithFields(fields).Infof("Disk usage check: wal=%.1f%% data=%.1f%%", r.walPercent, r.dataPercent)
	}
}

func sampleAge(now, observedAt time.Time) time.Duration {
	if observedAt.IsZero() {
		return 0
	}
	age := now.Sub(observedAt)
	if age < 0 {
		return 0
	}

	return age
}

func remoteVolumeValidity(volume *clusterpb.VolumeUsage) (bool, string) {
	if volume == nil {
		return false, "volume is missing from disk usage response"
	}
	if !volume.GetValid() {
		if volume.GetError() != "" {
			return false, volume.GetError()
		}

		return false, "latest collection attempt failed"
	}
	if volume.GetTotalBytes() == 0 {
		return false, "reported totalBytes is zero"
	}
	if volume.GetObservedAtUs() == 0 {
		return false, "reported observedAtUs is zero"
	}
	if !diskusage.SampleUsable(
		volume.GetValid(),
		volume.GetTotalBytes() > 0,
		volume.GetObservedAtUs() > 0,
		volume.GetSampleAgeMs(),
	) {
		return false, "disk usage sample is stale"
	}

	return true, ""
}

// safePercent returns the percentage of used/total as a float (0-100), or 0 if total is zero.
func safePercent(used, total uint64) float64 {
	if total == 0 {
		return 0
	}

	return float64(used) / float64(total) * 100
}

// safeFraction returns used/total in [0,1], or 0 when total is zero.
func safeFraction(used, total uint64) float64 {
	if total == 0 {
		return 0
	}

	return float64(used) / float64(total)
}

// logIfAtBlock emits the warning + antithesis assert for any volume of a node
// that is at or above its block threshold. It has no return value: the
// block/resume verdict is owned by Thresholds.NextDiskBlocked; this method
// exists purely to preserve the per-volume observability (loud signal when a
// volume crosses the high-water mark).
func (hc *HealthChecker) logIfAtBlock(nodeID uint64, walUsed, walTotal uint64, walValid bool, dataUsed, dataTotal uint64, dataValid bool) {
	if walValid {
		percent := float64(walUsed) / float64(walTotal)
		if percent >= hc.thresholds.WALBlock {
			details := map[string]any{
				"node_id": nodeID,
				"volume":  "wal",
				"used":    walUsed,
				"total":   walTotal,
				"percent": percent * 100,
			}

			assert.Unreachable("disk usage exceeds threshold", details)

			hc.logger.WithFields(details).
				Errorf("Disk usage exceeds threshold (%.0f%%)", hc.thresholds.WALBlock*100)
		}
	}

	if dataValid {
		percent := float64(dataUsed) / float64(dataTotal)
		if percent >= hc.thresholds.DataBlock {
			details := map[string]any{
				"node_id": nodeID,
				"volume":  "data",
				"used":    dataUsed,
				"total":   dataTotal,
				"percent": percent * 100,
			}

			assert.Unreachable("disk usage exceeds threshold", details)

			hc.logger.WithFields(details).
				Errorf("Disk usage exceeds threshold (%.0f%%)", hc.thresholds.DataBlock*100)
		}
	}
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
	rtt := afterCall.Sub(beforeCall)

	// If the RTT is too high (e.g. during reconnection), the midpoint estimate
	// is unreliable and would produce a spurious skew. Discard the sample.
	if rtt > hc.clockSkewThreshold/2 {
		if hc.logger.Enabled(logging.DebugLevel) {
			hc.logger.WithFields(map[string]any{
				"node_id": nodeID,
				"rtt":     rtt.String(),
			}).Debugf("Clock skew check discarded: RTT too high for reliable measurement")
		}

		return false
	}

	// Use the midpoint of the request as the local reference time to account for network RTT
	localTime := beforeCall.Add(rtt / 2)
	remoteTime := time.UnixMicro(int64(resp.GetTimestampUs()))

	skew := localTime.Sub(remoteTime)
	if skew < 0 {
		skew = -skew
	}

	if skew > hc.clockSkewThreshold {
		hc.logger.WithFields(map[string]any{
			"node_id": nodeID,
			"skew":    skew.String(),
			"rtt":     rtt.String(),
		}).Errorf("Clock skew exceeds threshold (%s)", hc.clockSkewThreshold)

		return true
	}

	return false
}
