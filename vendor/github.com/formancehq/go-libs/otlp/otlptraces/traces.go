package otlptraces

import (
	"context"

	"github.com/formancehq/go-libs/otlp"
	"go.opentelemetry.io/contrib/propagators/b3"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

const (
	StdoutExporter = "stdout"
	OTLPExporter   = "otlp"

	TracerProviderOptionKey = `group:"_tracerProviderOption"`
)

type JaegerConfig struct {
	Endpoint string
	User     string
	Password string
}

type OTLPConfig struct {
	Mode     string
	Endpoint string
	Insecure bool
}

type ModuleConfig struct {
	Exporter           string
	Batch              bool
	JaegerConfig       *JaegerConfig
	OTLPConfig         *OTLPConfig
	ResourceAttributes []string
	ServiceName        string
}

func ProvideTracerProviderOption(v any, annotations ...fx.Annotation) fx.Option {
	annotations = append(annotations, fx.ResultTags(TracerProviderOptionKey))
	return fx.Provide(fx.Annotate(v, annotations...))
}

func TracesModule(cfg ModuleConfig) fx.Option {
	options := make([]fx.Option, 0)
	options = append(options,
		fx.Supply(cfg),
		otlp.LoadResource(cfg.ServiceName, cfg.ResourceAttributes),
		fx.Provide(fx.Annotate(func(options ...tracesdk.TracerProviderOption) *tracesdk.TracerProvider {
			return tracesdk.NewTracerProvider(options...)
		}, fx.ParamTags(TracerProviderOptionKey))),
		fx.Provide(func(defaultTracerProvider *tracesdk.TracerProvider) trace.TracerProvider {
			return defaultTracerProvider
		}),
		fx.Invoke(func(tp trace.TracerProvider) trace.TracerProvider {
			otel.SetTracerProvider(tp)

			// set global propagator to tracecontext (the default is no-op).
			otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
				b3.New(), propagation.TraceContext{})) // B3 format is common and used by zipkin. Always enabled right now.

			return tp
		}),
		fx.Invoke(func(lc fx.Lifecycle, tracerProvider *tracesdk.TracerProvider) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return tracerProvider.Shutdown(ctx)
				},
			})
		}),
		ProvideTracerProviderOption(tracesdk.WithResource),
	)
	if cfg.Batch {
		options = append(options, ProvideTracerProviderOption(tracesdk.WithBatcher, fx.ParamTags(``, `group:"_batchOptions"`)))
	} else {
		options = append(options, ProvideTracerProviderOption(tracesdk.WithSyncer))
	}

	switch cfg.Exporter {
	case StdoutExporter:
		options = append(options, StdoutTracerModule())
	case OTLPExporter:
		options = append(options, OTLPTracerModule())
		mode := otlp.ModeGRPC
		if cfg.OTLPConfig != nil {
			if cfg.OTLPConfig.Mode != "" {
				mode = cfg.OTLPConfig.Mode
			}
			switch mode {
			case otlp.ModeGRPC:
				if cfg.OTLPConfig.Endpoint != "" {
					options = append(options, ProvideOTLPTracerGRPCClientOption(func() otlptracegrpc.Option {
						return otlptracegrpc.WithEndpoint(cfg.OTLPConfig.Endpoint)
					}))
				}
				if cfg.OTLPConfig.Insecure {
					options = append(options, ProvideOTLPTracerGRPCClientOption(func() otlptracegrpc.Option {
						return otlptracegrpc.WithInsecure()
					}))
				}
			case otlp.ModeHTTP:
				if cfg.OTLPConfig.Endpoint != "" {
					options = append(options, ProvideOTLPTracerHTTPClientOption(func() otlptracehttp.Option {
						return otlptracehttp.WithEndpoint(cfg.OTLPConfig.Endpoint)
					}))
				}
				if cfg.OTLPConfig.Insecure {
					options = append(options, ProvideOTLPTracerHTTPClientOption(func() otlptracehttp.Option {
						return otlptracehttp.WithInsecure()
					}))
				}
			}
		}
		switch mode {
		case otlp.ModeGRPC:
			options = append(options, OTLPTracerGRPCClientModule())
		case otlp.ModeHTTP:
			options = append(options, OTLPTracerHTTPClientModule())
		}
	}
	return fx.Options(options...)
}
