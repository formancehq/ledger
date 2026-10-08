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

// MaximumSampleAge is the oldest disk-usage sample that control loops may use.
// The collector normally refreshes every five seconds, so one minute tolerates
// transient scheduling delays without allowing an old successful Statfs call
// to drive health or expansion decisions indefinitely.
const MaximumSampleAge = time.Minute

// toInt64 converts any integer type to int64. This avoids unconvert lint errors
// when stat.Bsize is uint32 on darwin but int64 on linux.
func toInt64[T ~int32 | ~uint32 | ~int64 | ~uint64](v T) int64 {
	return int64(v)
}

// VolumeUsage holds the used and total bytes of a filesystem volume.
type VolumeUsage struct {
	sample atomic.Pointer[VolumeSample]
}

// VolumeSample is one atomic view of a volume's last known usage and the
// outcome of the latest collection attempt. A failed attempt preserves the
// last successful values and timestamp for diagnostics, but marks them invalid
// so control loops cannot act on stale data.
type VolumeSample struct {
	UsedBytes  int64
	TotalBytes int64
	ObservedAt time.Time
	Valid      bool
	Error      string
}

// Usable reports whether the sample is a recent successful measurement with a
// meaningful filesystem capacity. A small future timestamp is treated as age
// zero; local samples retain time.Time's monotonic component, so this is only a
// defensive clock-adjustment guard.
func (s VolumeSample) Usable(now time.Time) bool {
	if s.TotalBytes <= 0 {
		return false
	}
	age := max(now.Sub(s.ObservedAt), 0)

	return SampleUsable(
		s.Valid,
		true,
		!s.ObservedAt.IsZero(),
		uint64(age.Milliseconds()),
	)
}

// SampleUsable is the shared control-loop contract for local and remote disk
// samples. Age is passed in milliseconds so callers can preserve the
// server-computed age carried over the wire instead of recomputing it across
// clocks. Keeping the comparison in uint64 also makes an untrusted, overflowing
// wire age unambiguously stale.
func SampleUsable(valid, hasCapacity, hasObservation bool, sampleAgeMS uint64) bool {
	return valid && hasCapacity && hasObservation &&
		sampleAgeMS <= uint64(MaximumSampleAge.Milliseconds())
}

func (v *VolumeUsage) storeSuccess(used, total int64, observedAt time.Time) {
	v.sample.Store(&VolumeSample{
		UsedBytes:  used,
		TotalBytes: total,
		ObservedAt: observedAt,
		Valid:      true,
	})
}

func (v *VolumeUsage) storeFailure(err error) {
	sample := v.Load()
	sample.Valid = false
	sample.Error = err.Error()
	v.sample.Store(&sample)
}

// Load returns a consistent snapshot of the volume's measurement state.
func (v *VolumeUsage) Load() VolumeSample {
	sample := v.sample.Load()
	if sample == nil {
		return VolumeSample{}
	}

	return *sample
}

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
		c.WALVolume.storeSuccess(used, total, time.Now())
	} else {
		c.WALVolume.storeFailure(err)
	}

	if used, total, err := filesystemUsage(c.dataDir); err == nil {
		c.DataVolume.storeSuccess(used, total, time.Now())
	} else {
		c.DataVolume.storeFailure(err)
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
			if sample := c.WALVolume.Load(); sample.Valid {
				o.ObserveInt64(volumeGauge, sample.UsedBytes,
					metric.WithAttributes(volumeKey.String("wal")))
			}
			if sample := c.DataVolume.Load(); sample.Valid {
				o.ObserveInt64(volumeGauge, sample.UsedBytes,
					metric.WithAttributes(volumeKey.String("data")))
			}

			return nil
		},
		volumeGauge,
	)
}
