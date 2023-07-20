package otlpmetrics

import (
	"context"
	"time"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/otlp"
	"go.opentelemetry.io/contrib/instrumentation/host"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
)

const (
	metricsProviderOptionKey = `group:"_metricsProviderOption"`
	metricsRuntimeOptionKey  = `group:"_metricsRuntimeOption"`

	StdoutExporter = "stdout"
	OTLPExporter   = "otlp"
)

type ModuleConfig struct {
	ServiceName    string
	ServiceVersion string

	RuntimeMetrics              bool
	MinimumReadMemStatsInterval time.Duration

	Exporter           string
	OTLPConfig         *OTLPConfig
	PushInterval       time.Duration
	ResourceAttributes []string
}

type OTLPConfig struct {
	Mode     string
	Endpoint string
	Insecure bool
}

func ProvideMetricsProviderOption(v any, annotations ...fx.Annotation) fx.Option {
	annotations = append(annotations, fx.ResultTags(metricsProviderOptionKey))
	return fx.Provide(fx.Annotate(v, annotations...))
}

func ProvideRuntimeMetricsOption(v any, annotations ...fx.Annotation) fx.Option {
	annotations = append(annotations, fx.ResultTags(metricsRuntimeOptionKey))
	return fx.Provide(fx.Annotate(v, annotations...))

}

func MetricsModule(cfg ModuleConfig) fx.Option {
	options := make([]fx.Option, 0)
	options = append(options,
		fx.Supply(cfg),
		otlp.LoadResource(cfg.ServiceName, cfg.ResourceAttributes),
		fx.Decorate(fx.Annotate(func(mp *sdkmetric.MeterProvider) metric.MeterProvider { return mp }, fx.As(new(metric.MeterProvider)))),
		fx.Provide(fx.Annotate(func(options ...sdkmetric.Option) *sdkmetric.MeterProvider {
			return sdkmetric.NewMeterProvider(options...)
		}, fx.ParamTags(metricsProviderOptionKey))),
		fx.Invoke(func(lc fx.Lifecycle, metricProvider *sdkmetric.MeterProvider, options ...runtime.Option) {
			// set global propagator to tracecontext (the default is no-op).
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
				b3.New(), propagation.TraceContext{})) // B3 format is common and used by zipkin. Always enabled right now.
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					global.SetMeterProvider(metricProvider)
					if cfg.RuntimeMetrics {
						if err := runtime.Start(options...); err != nil {
							return err
						}
						if err := host.Start(); err != nil {
							return err
						}
					}
					return nil
				},
				OnStop: func(ctx context.Context) error {
					logging.FromContext(ctx).Infof("Flush metrics")
					if err := metricProvider.ForceFlush(ctx); err != nil {
						logging.FromContext(ctx).Errorf("unable to flush metrics: %s", err)
					}
					logging.FromContext(ctx).Infof("Shutting down metrics provider")
					if err := metricProvider.Shutdown(ctx); err != nil {
						logging.FromContext(ctx).Errorf("unable to shutdown metrics provider: %s", err)
					}
					logging.FromContext(ctx).Infof("Metrics provider stopped")
					return nil
				},
			})
		}),
		ProvideMetricsProviderOption(sdkmetric.WithResource),
		ProvideMetricsProviderOption(sdkmetric.WithReader),
		fx.Provide(
			fx.Annotate(sdkmetric.NewPeriodicReader, fx.As(new(sdkmetric.Reader))),
		),
		ProvideOTLPMetricsPeriodicReaderOption(func() sdkmetric.PeriodicReaderOption {
			return sdkmetric.WithInterval(cfg.PushInterval)
		}),
		ProvideRuntimeMetricsOption(func() runtime.Option {
			return runtime.WithMinimumReadMemStatsInterval(cfg.MinimumReadMemStatsInterval)
		}),
	)

	switch cfg.Exporter {
	case StdoutExporter:
		options = append(options, StdoutMetricsModule())
	case OTLPExporter:
		mode := otlp.ModeGRPC
		if cfg.OTLPConfig != nil {
			if cfg.OTLPConfig.Mode != "" {
				mode = cfg.OTLPConfig.Mode
			}
		}
		switch mode {
		case otlp.ModeGRPC:
			if cfg.OTLPConfig != nil {
				if cfg.OTLPConfig.Endpoint != "" {
					options = append(options, ProvideOTLPMetricsGRPCOption(func() otlpmetricgrpc.Option {
						return otlpmetricgrpc.WithEndpoint(cfg.OTLPConfig.Endpoint)
					}))
				}
				if cfg.OTLPConfig.Insecure {
					options = append(options, ProvideOTLPMetricsGRPCOption(func() otlpmetricgrpc.Option {
						return otlpmetricgrpc.WithInsecure()
					}))
				}
			}

			options = append(options, ProvideOTLPMetricsGRPCExporter())
		case otlp.ModeHTTP:
			if cfg.OTLPConfig != nil {
				if cfg.OTLPConfig.Endpoint != "" {
					options = append(options, ProvideOTLPMetricsHTTPOption(func() otlpmetrichttp.Option {
						return otlpmetrichttp.WithEndpoint(cfg.OTLPConfig.Endpoint)
					}))
				}
				if cfg.OTLPConfig.Insecure {
					options = append(options, ProvideOTLPMetricsHTTPOption(func() otlpmetrichttp.Option {
						return otlpmetrichttp.WithInsecure()
					}))
				}
			}

			options = append(options, ProvideOTLPMetricsHTTPExporter())
		}
	}

	return fx.Options(options...)
}
