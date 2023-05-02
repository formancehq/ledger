package otlpmetrics

import (
	"context"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
)

func LoadOTLPMetricsGRPCExporter(options ...otlpmetricgrpc.Option) (sdkmetric.Exporter, error) {
	// TODO(polo): context.Background() is not ideal here
	return otlpmetricgrpc.New(context.Background(), options...)
}

func ProvideOTLPMetricsGRPCExporter() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadOTLPMetricsGRPCExporter, fx.As(new(sdkmetric.Exporter))),
		),
	)
}

func LoadOTLPMetricsHTTPExporter(options ...otlpmetrichttp.Option) (sdkmetric.Exporter, error) {
	// TODO(polo): context.Background() is not ideal here
	return otlpmetrichttp.New(context.Background(), options...)
}

func ProvideOTLPMetricsHTTPExporter() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadOTLPMetricsHTTPExporter, fx.As(new(sdkmetric.Exporter))),
		),
	)
}

const OTLPMetricsGRPCOptionsKey = `group:"_otlpMetricsGrpcOptions"`

func ProvideOTLPMetricsGRPCOption(provider any) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(OTLPMetricsGRPCOptionsKey), fx.As(new(otlpmetricgrpc.Option))),
	)
}

const OTLPMetricsHTTPOptionsKey = `group:"_otlpMetricsHTTPOptions"`

func ProvideOTLPMetricsHTTPOption(provider any) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(OTLPMetricsHTTPOptionsKey), fx.As(new(otlpmetrichttp.Option))),
	)
}

const OTLPMetricsPeriodicReaderOptionsKey = `group:"_otlpMetricsPeriodicReaderOptions"`

func ProvideOTLPMetricsPeriodicReaderOption(provider any) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(OTLPMetricsPeriodicReaderOptionsKey), fx.As(new(sdkmetric.PeriodicReaderOption))),
	)
}
