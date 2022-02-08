package opentelemetrytraces

import (
	"fmt"
	"github.com/XSAM/otelsql"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.uber.org/fx"
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
	JaegerConfig *JaegerConfig
	OTLPConfig   *OTLPConfig
}

func TracesModule(cfg ModuleConfig) fx.Option {
	options := make([]fx.Option, 0)
	options = append(options,
		ResourceFactoryModule(),
		ProvideOTLPAttribute(semconv.ServiceNameKey.String(cfg.ServiceName)),
		ProvideOTLPAttribute(semconv.ServiceVersionKey.String(cfg.Version)),
	)

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
	case NoOpExporter:
		options = append(options, NoOpTracerModule())
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
