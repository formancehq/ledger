package dal

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func NewMetricsListener(m metric.Meter) *pebble.EventListener {
	flushTotal, err := m.Int64Counter(
		"pebble.flush.total",
		metric.WithDescription("Number of Pebble flush operations"),
	)
	if err != nil {
		panic(err)
	}
	flushDurMilliseconds, err := m.Int64Histogram(
		"pebble.flush.duration.milliseconds",
		metric.WithUnit("ms"),
		metric.WithDescription("Duration of Pebble flush operations"),
	)
	if err != nil {
		panic(err)
	}
	flushInputBytes, err := m.Int64Histogram(
		"pebble.flush.input.bytes",
		metric.WithUnit("By"),
		metric.WithDescription("Input bytes flushed from memtables"),
	)
	if err != nil {
		panic(err)
	}

	compactionTotal, err := m.Int64Counter(
		"pebble.compaction.total",
		metric.WithDescription("Number of Pebble compaction operations"),
	)
	if err != nil {
		panic(err)
	}
	compactionMilliseconds, err := m.Int64Histogram(
		"pebble.compaction.duration.milliseconds",
		metric.WithUnit("ms"),
		metric.WithDescription("Duration of Pebble compactions"),
	)
	if err != nil {
		panic(err)
	}

	stallTotal, err := m.Int64Counter(
		"pebble.write_stall.total",
		metric.WithDescription("Number of Pebble write stalls"),
	)
	if err != nil {
		panic(err)
	}
	stallMilliseconds, err := m.Int64Histogram(
		"pebble.write_stall.duration.milliseconds",
		metric.WithUnit("ms"),
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
		FlushEnd: func(info pebble.FlushInfo) {
			attrs := []attribute.KeyValue{
				attribute.String("reason", info.Reason),
				attribute.String("status", statusFromErr(info.Err)),
			}

			flushTotal.Add(ctx, 1, metric.WithAttributes(attrs...))

			// Prefer info.Duration (CPU+IO), not TotalDuration, for "work time".
			flushDurMilliseconds.Record(ctx, info.Duration.Milliseconds(), metric.WithAttributes(attrs...))
			flushInputBytes.Record(ctx, int64(info.InputBytes), metric.WithAttributes(attrs...))
		},

		CompactionEnd: func(info pebble.CompactionInfo) {
			attrs := []attribute.KeyValue{
				attribute.String("reason", info.Reason),
				attribute.String("status", statusFromErr(info.Err)),
			}

			compactionTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
			compactionMilliseconds.Record(ctx, info.Duration.Milliseconds(), metric.WithAttributes(attrs...))
		},

		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
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
			stallMilliseconds.Record(ctx, d.Milliseconds(), metric.WithAttributes(attrs...))
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
