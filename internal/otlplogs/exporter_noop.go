package otlplogs

import (
	"context"

	"go.opentelemetry.io/otel/sdk/log"
)

type NoOpExporter struct{}

func (n NoOpExporter) Export(ctx context.Context, records []log.Record) error {
	return nil
}

func (n NoOpExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (n NoOpExporter) ForceFlush(ctx context.Context) error {
	return nil
}

func NewNoOpExporter() *NoOpExporter {
	return &NoOpExporter{}
}

var _ log.Exporter = (*NoOpExporter)(nil)
