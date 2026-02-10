package diskusage

import (
	"context"
	"io/fs"
	"path/filepath"
	"strings"

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

// RegisterMetrics registers observable gauges for disk space consumption.
// It returns a metric.Registration that can be used to unregister the callbacks.
func RegisterMetrics(meter metric.Meter, walDir, dataDir string) (metric.Registration, error) {
	spoolDir := filepath.Join(walDir, "spool")

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
			// Per-component metrics
			if size, err := DirSize(spoolDir); err == nil {
				o.ObserveInt64(componentGauge, size,
					metric.WithAttributes(componentKey.String("spool")))
			}
			if size, err := dirSizeExcluding(walDir, spoolDir); err == nil {
				o.ObserveInt64(componentGauge, size,
					metric.WithAttributes(componentKey.String("wal")))
			}
			if size, err := DirSize(dataDir); err == nil {
				o.ObserveInt64(componentGauge, size,
					metric.WithAttributes(componentKey.String("data")))
			}

			// Per-volume metrics
			if size, err := DirSize(walDir); err == nil {
				o.ObserveInt64(volumeGauge, size,
					metric.WithAttributes(volumeKey.String("wal")))
			}
			if size, err := DirSize(dataDir); err == nil {
				o.ObserveInt64(volumeGauge, size,
					metric.WithAttributes(volumeKey.String("data")))
			}

			return nil
		},
		componentGauge,
		volumeGauge,
	)
}
