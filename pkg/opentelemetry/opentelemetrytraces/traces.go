package opentelemetrytraces

import (
	"fmt"
	"github.com/XSAM/otelsql"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

type JaegerConfig struct {
	Endpoint string
	User     string
	Password string
}

type OTLPTracesConfig struct {
	Mode     string
	Endpoint string
	Insecure bool
}

type TracesModuleConfig struct {
	ServiceName       string
	Version           string
	Exporter          string
	JaegerConfig      *JaegerConfig
	OTLPConfig        *OTLPTracesConfig
	ApiMiddlewareName string
}

func TracesModule(cfg TracesModuleConfig) fx.Option {
	options := make([]fx.Option, 0)
	options = append(options,
		ProvideServiceName(func() string { return "ledger" }),
		ProvideServiceVersion(func() string { return cfg.Version }),
	)

	options = append(options, fx.Invoke(func(cfg struct {
		fx.In
		Flavor sqlstorage.Flavor `optional:"true"`
	}) error {
		if cfg.Flavor != 0 {
			sqlDriverName, err := otelsql.Register(
				sqlstorage.SQLDriverName(cfg.Flavor),
				cfg.Flavor.AttributeKeyValue().Value.AsString(),
			)
			if err != nil {
				return fmt.Errorf("Error registering otel driver: %s", err)
			}
			sqlstorage.UpdateSQLDriverMapping(cfg.Flavor, sqlDriverName)
		}
		return nil
	}))

	switch cfg.Exporter {
	case JaegerTracesExporter:
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
	case StdoutTracesExporter:
		options = append(options, StdoutTracerModule())
	case NoOpTracesExporter:
		options = append(options, NoOpTracerModule())
	case OTLPTracesExporter:
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
					options = append(options, ProvideOTLPTracerGRPCClientOption(func() otlptracehttp.Option {
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
	if cfg.ApiMiddlewareName != "" {
		options = append(options, routes.ProvideGlobalMiddleware(func(tracerProvider trace.TracerProvider) gin.HandlerFunc {
			return otelgin.Middleware("ledger", otelgin.WithTracerProvider(tracerProvider))
		}))
	}
	return fx.Options(options...)
}
