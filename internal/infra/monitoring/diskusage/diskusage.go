package diskusage

import (
	"context"
	"fmt"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/formancehq/ledger/v3/internal/pkg/worker"
)

// toInt64 converts any integer type to int64. This avoids unconvert lint errors
// when stat.Bsize is uint32 on darwin but int64 on linux.
func toInt64[T ~int32 | ~uint32 | ~int64 | ~uint64](v T) int64 {
	return int64(v)
}

// VolumeUsage holds the used and total bytes of a filesystem volume.
type VolumeUsage struct {
	usedBytes  atomic.Int64
	totalBytes atomic.Int64
}

func (v *VolumeUsage) store(used, total int64) {
	v.usedBytes.Store(used)
	v.totalBytes.Store(total)
}

// UsedBytes returns the last computed used bytes on the filesystem.
func (v *VolumeUsage) UsedBytes() int64 { return v.usedBytes.Load() }

// TotalBytes returns the total capacity of the filesystem in bytes.
func (v *VolumeUsage) TotalBytes() int64 { return v.totalBytes.Load() }

var volumeKey = attribute.Key("volume")

// Collector periodically computes filesystem-level disk usage via syscall.Statfs
// and exposes cached values to OTEL observable gauge callbacks.
type Collector struct {
	walDir  string
	dataDir string

	interval time.Duration
	meter    metric.Meter

	WALVolume  VolumeUsage
	DataVolume VolumeUsage

	metricsRegistration metric.Registration
	w                   worker.Worker
}

// NewCollector creates a new Collector that will periodically compute filesystem
// usage for the WAL and data volumes at the specified interval via syscall.Statfs.
func NewCollector(walDir, dataDir string, interval time.Duration, meter metric.Meter) *Collector {
	return &Collector{
		walDir:   walDir,
		dataDir:  dataDir,
		interval: interval,
		meter:    meter,
		w:        worker.New(),
	}
}

// Start registers OTEL metrics, performs an initial collection, and launches
// the background goroutine that periodically computes filesystem usage.
func (c *Collector) Start() {
	// Best-effort metrics registration — failure is not fatal.
	if reg, err := c.registerMetrics(); err == nil {
		c.metricsRegistration = reg
	}

	c.collect()
	c.w.Run(func(stop <-chan struct{}) {
		worker.RunTicker(stop, c.interval, c.collect)
	})
}

// Stop signals the background goroutine to stop, waits for it to finish,
// and unregisters OTEL metrics.
func (c *Collector) Stop() {
	c.w.Stop()

	if c.metricsRegistration != nil {
		_ = c.metricsRegistration.Unregister()
	}
}

// collect reads filesystem usage via syscall.Statfs and stores results atomically.
func (c *Collector) collect() {
	if used, total, err := filesystemUsage(c.walDir); err == nil {
		c.WALVolume.store(used, total)
	}

	if used, total, err := filesystemUsage(c.dataDir); err == nil {
		c.DataVolume.store(used, total)
	}
}

// filesystemUsage returns the used and total bytes of the filesystem containing path.
// Used bytes are computed as (Blocks - Bavail) * Bsize, which accounts for all
// consumers on the filesystem (not just managed directories) and includes space
// reserved for root. This is a single syscall, much faster than walking directories.
func filesystemUsage(path string) (used, total int64, err error) {
	var stat syscall.Statfs_t

	if err = syscall.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}

	bsize := toInt64(stat.Bsize)
	total = int64(stat.Blocks) * bsize
	used = int64(stat.Blocks-stat.Bavail) * bsize

	return used, total, nil
}

// registerMetrics registers observable gauges for disk space consumption.
// The callback reads cached values computed by the background goroutine.
func (c *Collector) registerMetrics() (metric.Registration, error) {
	volumeGauge, err := c.meter.Int64ObservableGauge(
		"storage.disk.volume.bytes",
		metric.WithDescription("Disk space used by a storage volume"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating volume gauge: %w", err)
	}

	return c.meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(volumeGauge, c.WALVolume.UsedBytes(),
				metric.WithAttributes(volumeKey.String("wal")))
			o.ObserveInt64(volumeGauge, c.DataVolume.UsedBytes(),
				metric.WithAttributes(volumeKey.String("data")))

			return nil
		},
		volumeGauge,
	)
}
