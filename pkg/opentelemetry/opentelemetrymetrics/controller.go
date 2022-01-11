package opentelemetrymetrics

import (
	"context"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric"
	"go.opentelemetry.io/otel/metric"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/metric/selector/simple"
	"go.uber.org/fx"
	"time"
)

func LoadController(exp *otlpmetric.Exporter) *controller.Controller {
	return controller.New(
		processor.NewFactory(simple.NewWithHistogramDistribution(), exp),
		controller.WithExporter(exp),
		controller.WithCollectPeriod(2*time.Second),
	)
}

func MetricsControllerModule() fx.Option {
	return fx.Options(
		fx.Provide(LoadController),
		fx.Provide(func(ctrl *controller.Controller) metric.MeterProvider {
			return ctrl
		}),
		fx.Invoke(func(lc fx.Lifecycle, pusher *controller.Controller) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return pusher.Start(context.Background())
				},
				OnStop: func(ctx context.Context) error {
					return pusher.Stop(ctx)
				},
			})
		}),
	)
}
