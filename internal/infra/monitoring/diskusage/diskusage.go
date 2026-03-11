package diskusage

import (
	"context"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
)

// DirSize computes the total size in bytes of all files under the given directory.
func DirSize(path string) (int64, error) {
	var size int64

	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		size += info.Size()

		return nil
	})

	return size, err
}

// dirSizeExcluding computes the total size in bytes of all files under path,
// excluding any files whose path starts with the given prefix.
func dirSizeExcluding(path, excludePrefix string) (int64, error) {
	var size int64

	err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		if strings.HasPrefix(p, excludePrefix) {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		size += info.Size()

		return nil
	})

	return size, err
}

var (
	componentKey = attribute.Key("component")
	volumeKey    = attribute.Key("volume")
)

// Collector periodically computes directory sizes in the background and
// exposes cached values to OTEL observable gauge callbacks.
type Collector struct {
	walDir        string
	dataDir       string
	spoolDir      string
	readIndexDir  string
	readIndexPath string
	interval      time.Duration
	meter         metric.Meter

	spoolBytes           atomic.Int64
	walBytes             atomic.Int64
	dataBytes            atomic.Int64
	readIndexBytes       atomic.Int64
	readIndexMmapRSS     atomic.Int64
	walVolumeBytes       atomic.Int64
	dataVolumeBytes      atomic.Int64
	walVolumeTotalBytes  atomic.Int64
	dataVolumeTotalBytes atomic.Int64

	metricsRegistration metric.Registration
	w                   worker.Worker
}

// NewCollector creates a new Collector that will periodically compute disk usage
// for the given directories at the specified interval. readIndexPath is the path
// to the Pebble database file (e.g. "<dir>/readindex.db") used to measure mmap
// RSS on Linux. The meter is used to register OTEL observable gauges on Start
// and unregister them on Stop.
func NewCollector(walDir, dataDir, readIndexDir, readIndexPath string, interval time.Duration, meter metric.Meter) *Collector {
	return &Collector{
		walDir:        walDir,
		dataDir:       dataDir,
		spoolDir:      filepath.Join(walDir, "spool"),
		readIndexDir:  readIndexDir,
		readIndexPath: readIndexPath,
		interval:      interval,
		meter:         meter,
		w:             worker.New(),
	}
}

// Start registers OTEL metrics, performs an initial collection, and launches
// the background goroutine that periodically computes directory sizes.
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

// collect computes directory sizes and stores the results atomically.
func (c *Collector) collect() {
	if size, err := DirSize(c.spoolDir); err == nil {
		c.spoolBytes.Store(size)
	}

	if size, err := dirSizeExcluding(c.walDir, c.spoolDir); err == nil {
		c.walBytes.Store(size)
	}

	if size, err := DirSize(c.readIndexDir); err == nil {
		c.readIndexBytes.Store(size)
	}

	if rss, err := mmapRSSBytes(c.readIndexPath); err == nil {
		c.readIndexMmapRSS.Store(rss)
	}

	if size, err := dirSizeExcluding(c.dataDir, c.readIndexDir); err == nil {
		c.dataBytes.Store(size)
	}

	if size, err := DirSize(c.walDir); err == nil {
		c.walVolumeBytes.Store(size)
	}

	if size, err := DirSize(c.dataDir); err == nil {
		c.dataVolumeBytes.Store(size)
	}

	if total, err := filesystemTotalBytes(c.walDir); err == nil {
		c.walVolumeTotalBytes.Store(total)
	}

	if total, err := filesystemTotalBytes(c.dataDir); err == nil {
		c.dataVolumeTotalBytes.Store(total)
	}
}

// filesystemTotalBytes returns the total capacity of the filesystem containing path.
func filesystemTotalBytes(path string) (int64, error) {
	var stat syscall.Statfs_t

	err := syscall.Statfs(path, &stat)
	if err != nil {
		return 0, err
	}

	return int64(stat.Blocks) * int64(stat.Bsize), nil
}

// SpoolBytes returns the last computed spool directory size in bytes.
func (c *Collector) SpoolBytes() int64 { return c.spoolBytes.Load() }

// WALBytes returns the last computed WAL directory size in bytes (excluding spool).
func (c *Collector) WALBytes() int64 { return c.walBytes.Load() }

// DataBytes returns the last computed data directory size in bytes (excluding read index).
func (c *Collector) DataBytes() int64 { return c.dataBytes.Load() }

// ReadIndexBytes returns the last computed Pebble read index size in bytes.
func (c *Collector) ReadIndexBytes() int64 { return c.readIndexBytes.Load() }

// ReadIndexMmapRSSBytes returns the resident set size of the Pebble mmap in bytes.
// On non-Linux platforms this always returns 0.
func (c *Collector) ReadIndexMmapRSSBytes() int64 { return c.readIndexMmapRSS.Load() }

// WALVolumeBytes returns the last computed total WAL volume size in bytes.
func (c *Collector) WALVolumeBytes() int64 { return c.walVolumeBytes.Load() }

// DataVolumeBytes returns the last computed total data volume size in bytes.
func (c *Collector) DataVolumeBytes() int64 { return c.dataVolumeBytes.Load() }

// WALVolumeTotalBytes returns the total capacity of the WAL filesystem in bytes.
func (c *Collector) WALVolumeTotalBytes() int64 { return c.walVolumeTotalBytes.Load() }

// DataVolumeTotalBytes returns the total capacity of the data filesystem in bytes.
func (c *Collector) DataVolumeTotalBytes() int64 { return c.dataVolumeTotalBytes.Load() }

// registerMetrics registers observable gauges for disk space consumption.
// The callback reads cached values computed by the background goroutine.
func (c *Collector) registerMetrics() (metric.Registration, error) {
	componentGauge, err := c.meter.Int64ObservableGauge(
		"storage.disk.component.bytes",
		metric.WithDescription("Disk space used by storage component"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating component gauge: %w", err)
	}

	volumeGauge, err := c.meter.Int64ObservableGauge(
		"storage.disk.volume.bytes",
		metric.WithDescription("Disk space used by storage volume"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating volume gauge: %w", err)
	}

	mmapRSSGauge, err := c.meter.Int64ObservableGauge(
		"storage.mmap.rss.bytes",
		metric.WithDescription("Resident set size of memory-mapped storage files"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("creating mmap RSS gauge: %w", err)
	}

	return c.meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(componentGauge, c.spoolBytes.Load(),
				metric.WithAttributes(componentKey.String("spool")))
			o.ObserveInt64(componentGauge, c.walBytes.Load(),
				metric.WithAttributes(componentKey.String("wal")))
			o.ObserveInt64(componentGauge, c.dataBytes.Load(),
				metric.WithAttributes(componentKey.String("data")))
			o.ObserveInt64(componentGauge, c.readIndexBytes.Load(),
				metric.WithAttributes(componentKey.String("readindex")))

			o.ObserveInt64(volumeGauge, c.walVolumeBytes.Load(),
				metric.WithAttributes(volumeKey.String("wal")))
			o.ObserveInt64(volumeGauge, c.dataVolumeBytes.Load(),
				metric.WithAttributes(volumeKey.String("data")))

			o.ObserveInt64(mmapRSSGauge, c.readIndexMmapRSS.Load(),
				metric.WithAttributes(componentKey.String("readindex")))

			return nil
		},
		componentGauge,
		volumeGauge,
		mmapRSSGauge,
	)
}
