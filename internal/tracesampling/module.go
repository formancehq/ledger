package tracesampling

import (
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

// Module returns an fx.Option that provides the trace sampling functionality.
// This module wraps trace exporters with error-aware sampling when enabled.
//
// The module uses fx.Decorate to wrap the SpanExporter provided by otlptraces
// with error-aware filtering that:
// - Always exports spans with errors
// - Applies ratio-based sampling to successful spans
func Module(cfg Config) fx.Option {
	if !cfg.Enabled {
		return fx.Options()
	}

	return fx.Options(
		fx.Supply(cfg),
		fx.Decorate(func(cfg Config, exporter sdktrace.SpanExporter) sdktrace.SpanExporter {
			return NewErrorAwareSamplingExporter(exporter, cfg.SuccessRatio)
		}),
	)
}

// WrapExporter wraps an existing SpanExporter with error-aware sampling.
// This can be used when fx decoration is not possible.
func WrapExporter(exporter sdktrace.SpanExporter, cfg Config) sdktrace.SpanExporter {
	if !cfg.Enabled {
		return exporter
	}
	return NewErrorAwareSamplingExporter(exporter, cfg.SuccessRatio)
}
