package opentelemetrytraces

import (
	"context"
	"fmt"
	"github.com/XSAM/otelsql"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

const (
	JaegerExporter = "jaeger"
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
	ServiceName  string
	Version      string
	Exporter     string
	Batch        bool
	JaegerConfig *JaegerConfig
	OTLPConfig   *OTLPConfig
}

func ProvideTracerProviderOption(v interface{}, annotations ...fx.Annotation) fx.Option {
	annotations = append(annotations, fx.ResultTags(TracerProviderOptionKey))
	return fx.Provide(fx.Annotate(v, annotations...))
}

func TracesModule(cfg ModuleConfig) fx.Option {
	options := make([]fx.Option, 0)
	options = append(options,
		ResourceFactoryModule(),
		ProvideOTLPAttribute(semconv.ServiceNameKey.String(cfg.ServiceName)),
		ProvideOTLPAttribute(semconv.ServiceVersionKey.String(cfg.Version)),
		fx.Provide(func(tp *tracesdk.TracerProvider) trace.TracerProvider { return tp }),
		fx.Provide(fx.Annotate(func(options ...tracesdk.TracerProviderOption) *tracesdk.TracerProvider {
			return tracesdk.NewTracerProvider(options...)
		}, fx.ParamTags(TracerProviderOptionKey))),
		fx.Invoke(func(lc fx.Lifecycle, tracerProvider *tracesdk.TracerProvider) {
			// set global propagator to tracecontext (the default is no-op).
			otel.SetTextMapPropagator(propagation.TraceContext{})
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					otel.SetTracerProvider(tracerProvider)
					return nil
				},
				OnStop: func(ctx context.Context) error {
					return tracerProvider.Shutdown(ctx)
				},
			})
		}),
		fx.Provide(func(factory *resourceFactory) (*resource.Resource, error) {
			return factory.Make()
		}),
		ProvideTracerProviderOption(tracesdk.WithResource),
	)
	if cfg.Batch {
		options = append(options, ProvideTracerProviderOption(tracesdk.WithBatcher, fx.ParamTags(``, `group:"_batchOptions"`)))
	} else {
		options = append(options, ProvideTracerProviderOption(tracesdk.WithSyncer))
	}

	options = append(options, fx.Invoke(func(cfg struct {
		fx.In
		Flavor sqlstorage.Flavor `optional:"true"`
	}) error {
		if cfg.Flavor != 0 {
			var attr attribute.KeyValue
			switch cfg.Flavor {
			case sqlstorage.SQLite:
				attr = semconv.DBSystemSqlite
			case sqlstorage.PostgreSQL:
				attr = semconv.DBSystemPostgreSQL
			}
			sqlDriverName, err := otelsql.Register(sqlstorage.SQLDriverName(cfg.Flavor), attr.Value.AsString())
			if err != nil {
				return fmt.Errorf("Error registering otel driver: %s", err)
			}
			sqlstorage.UpdateSQLDriverMapping(cfg.Flavor, sqlDriverName)
		}
		return nil
	}))

	switch cfg.Exporter {
	case JaegerExporter:
		options = append(options, JaegerTracerModule())
		if cfg.JaegerConfig != nil {
			if v := cfg.JaegerConfig.Endpoint; v != "" {
				options = append(options, ProvideJaegerTracerCollectorEndpoint(func() jaeger.CollectorEndpointOption {
					return jaeger.WithEndpoint(v)
				}))
			}

			if v := cfg.JaegerConfig.User; v != "" {
				options = append(options, ProvideJaegerTracerCollectorEndpoint(func() jaeger.CollectorEndpointOption {
					return jaeger.WithUsername(v)
				}))
			}

			if v := cfg.JaegerConfig.Password; v != "" {
				options = append(options, ProvideJaegerTracerCollectorEndpoint(func() jaeger.CollectorEndpointOption {
					return jaeger.WithPassword(v)
				}))
			}
		}
	case StdoutExporter:
		options = append(options, StdoutTracerModule())
	case OTLPExporter:
		options = append(options, OTLPTracerModule())
		mode := opentelemetry.ModeGRPC
		if cfg.OTLPConfig != nil {
			if cfg.OTLPConfig.Mode != "" {
				mode = cfg.OTLPConfig.Mode
			}
			switch mode {
			case opentelemetry.ModeGRPC:
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
			case opentelemetry.ModeHTTP:
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
		case opentelemetry.ModeGRPC:
			options = append(options, OTLPTracerGRPCClientModule())
		case opentelemetry.ModeHTTP:
			options = append(options, OTLPTracerHTTPClientModule())
		}
	}
	return fx.Options(options...)
}
