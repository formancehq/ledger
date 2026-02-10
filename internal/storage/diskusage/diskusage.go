package diskusage

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
	walDir   string
	dataDir  string
	spoolDir string
	interval time.Duration

	spoolBytes      atomic.Int64
	walBytes        atomic.Int64
	dataBytes       atomic.Int64
	walVolumeBytes  atomic.Int64
	dataVolumeBytes atomic.Int64

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewCollector creates a new Collector that will periodically compute disk usage
// for the given directories at the specified interval.
func NewCollector(walDir, dataDir string, interval time.Duration) *Collector {
	return &Collector{
		walDir:   walDir,
		dataDir:  dataDir,
		spoolDir: filepath.Join(walDir, "spool"),
		interval: interval,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
}

// Start launches the background goroutine that periodically computes directory sizes.
func (c *Collector) Start() {
	c.collect()

	go func() {
		defer close(c.doneCh)
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()

		for {
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
				c.collect()
			}
		}
	}()
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (c *Collector) Stop() {
	close(c.stopCh)
	<-c.doneCh
}

// collect computes directory sizes and stores the results atomically.
func (c *Collector) collect() {
	if size, err := DirSize(c.spoolDir); err == nil {
		c.spoolBytes.Store(size)
	}
	if size, err := dirSizeExcluding(c.walDir, c.spoolDir); err == nil {
		c.walBytes.Store(size)
	}
	if size, err := DirSize(c.dataDir); err == nil {
		c.dataBytes.Store(size)
	}
	if size, err := DirSize(c.walDir); err == nil {
		c.walVolumeBytes.Store(size)
	}
	if size, err := DirSize(c.dataDir); err == nil {
		c.dataVolumeBytes.Store(size)
	}
}

// SpoolBytes returns the last computed spool directory size in bytes.
func (c *Collector) SpoolBytes() int64 { return c.spoolBytes.Load() }

// WALBytes returns the last computed WAL directory size in bytes (excluding spool).
func (c *Collector) WALBytes() int64 { return c.walBytes.Load() }

// DataBytes returns the last computed data directory size in bytes.
func (c *Collector) DataBytes() int64 { return c.dataBytes.Load() }

// WALVolumeBytes returns the last computed total WAL volume size in bytes.
func (c *Collector) WALVolumeBytes() int64 { return c.walVolumeBytes.Load() }

// DataVolumeBytes returns the last computed total data volume size in bytes.
func (c *Collector) DataVolumeBytes() int64 { return c.dataVolumeBytes.Load() }

// RegisterMetrics registers observable gauges for disk space consumption.
// The callback reads cached values computed by the background goroutine.
func (c *Collector) RegisterMetrics(meter metric.Meter) (metric.Registration, error) {
	componentGauge, err := meter.Int64ObservableGauge(
		"storage.disk.component.bytes",
		metric.WithDescription("Disk space used by storage component"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	volumeGauge, err := meter.Int64ObservableGauge(
		"storage.disk.volume.bytes",
		metric.WithDescription("Disk space used by storage volume"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, err
	}

	return meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			o.ObserveInt64(componentGauge, c.spoolBytes.Load(),
				metric.WithAttributes(componentKey.String("spool")))
			o.ObserveInt64(componentGauge, c.walBytes.Load(),
				metric.WithAttributes(componentKey.String("wal")))
			o.ObserveInt64(componentGauge, c.dataBytes.Load(),
				metric.WithAttributes(componentKey.String("data")))

			o.ObserveInt64(volumeGauge, c.walVolumeBytes.Load(),
				metric.WithAttributes(volumeKey.String("wal")))
			o.ObserveInt64(volumeGauge, c.dataVolumeBytes.Load(),
				metric.WithAttributes(volumeKey.String("data")))

			return nil
		},
		componentGauge,
		volumeGauge,
	)
}
