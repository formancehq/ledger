package dal

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func NewMetricsListener(m metric.Meter, stallState *WriteStallState) *pebble.EventListener {
	diskSlowTotal, err := m.Int64Counter(
		"pebble.disk_slow.operations",
		metric.WithUnit("{operation}"),
		metric.WithDescription("Number of Pebble disk slow events"),
	)
	if err != nil {
		panic(err)
	}

	diskSlowDuration, err := m.Float64Histogram(
		"pebble.disk_slow.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of slow disk operations detected by Pebble"),
	)
	if err != nil {
		panic(err)
	}

	flushTotal, err := m.Int64Counter(
		"pebble.flushes",
		metric.WithUnit("{flush}"),
		metric.WithDescription("Number of Pebble flush operations"),
	)
	if err != nil {
		panic(err)
	}

	flushDuration, err := m.Float64Histogram(
		"pebble.flush.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of Pebble flush operations"),
	)
	if err != nil {
		panic(err)
	}

	flushInputBytes, err := m.Int64Histogram(
		"pebble.flush.input.size",
		metric.WithUnit("By"),
		metric.WithDescription("Input bytes flushed from memtables"),
	)
	if err != nil {
		panic(err)
	}

	compactionTotal, err := m.Int64Counter(
		"pebble.compactions",
		metric.WithUnit("{compaction}"),
		metric.WithDescription("Number of Pebble compaction operations"),
	)
	if err != nil {
		panic(err)
	}

	compactionDuration, err := m.Float64Histogram(
		"pebble.compaction.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of Pebble compactions"),
	)
	if err != nil {
		panic(err)
	}

	stallTotal, err := m.Int64Counter(
		"pebble.write_stalls",
		metric.WithUnit("{stall}"),
		metric.WithDescription("Number of Pebble write stalls"),
	)
	if err != nil {
		panic(err)
	}

	stallDuration, err := m.Float64Histogram(
		"pebble.write_stall.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of Pebble write stalls"),
	)
	if err != nil {
		panic(err)
	}

	stallActiveGauge, err := m.Int64Gauge(
		"pebble.write_stall.active",
		metric.WithDescription("Whether Pebble is currently stalling writes (1/0)"),
	)
	if err != nil {
		panic(err)
	}

	var (
		stallStart time.Time
		stallOn    bool
		stallAttrs []attribute.KeyValue
		mu         sync.Mutex
		ctx        = context.Background()
	)

	return &pebble.EventListener{
		DiskSlow: func(info pebble.DiskSlowInfo) {
			attrs := []attribute.KeyValue{
				attribute.String("op", info.OpType.String()),
			}

			diskSlowTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
			diskSlowDuration.Record(ctx, info.Duration.Seconds(), metric.WithAttributes(attrs...))
		},

		FlushEnd: func(info pebble.FlushInfo) {
			attrs := []attribute.KeyValue{
				attribute.String("reason", info.Reason),
				attribute.String("status", statusFromErr(info.Err)),
			}

			flushTotal.Add(ctx, 1, metric.WithAttributes(attrs...))

			// Prefer info.Duration (CPU+IO), not TotalDuration, for "work time".
			flushDuration.Record(ctx, info.Duration.Seconds(), metric.WithAttributes(attrs...))
			flushInputBytes.Record(ctx, int64(info.InputBytes), metric.WithAttributes(attrs...))
		},

		CompactionEnd: func(info pebble.CompactionInfo) {
			attrs := []attribute.KeyValue{
				attribute.String("reason", info.Reason),
				attribute.String("status", statusFromErr(info.Err)),
			}

			compactionTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
			compactionDuration.Record(ctx, info.Duration.Seconds(), metric.WithAttributes(attrs...))
		},

		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			stallState.OnStallBegin()

			attrs := []attribute.KeyValue{
				attribute.String("reason", info.Reason),
			}

			stallTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
			stallActiveGauge.Record(ctx, 1, metric.WithAttributes(attrs...))

			// measure duration until WriteStallEnd
			mu.Lock()
			// if Pebble ever triggers nested stalls, keep first start
			if !stallOn {
				stallOn = true
				stallStart = time.Now()
				stallAttrs = attrs
			}
			mu.Unlock()
		},

		WriteStallEnd: func() {
			stallState.OnStallEnd()

			mu.Lock()
			if !stallOn {
				mu.Unlock()
				// best effort: still record gauge down with base attrs
				stallActiveGauge.Record(ctx, 0)

				return
			}

			start := stallStart
			attrs := stallAttrs
			stallOn = false
			stallAttrs = nil
			mu.Unlock()

			d := time.Since(start)
			stallDuration.Record(ctx, d.Seconds(), metric.WithAttributes(attrs...))
			// gauge down (same attrs as begin if possible)
			stallActiveGauge.Record(ctx, 0, metric.WithAttributes(attrs...))
		},
	}
}

func statusFromErr(err error) string {
	if err == nil {
		return "ok"
	}

	return "error"
}
