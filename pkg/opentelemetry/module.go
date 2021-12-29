package opentelemetry

import (
	"fmt"
	"github.com/XSAM/otelsql"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

const (
	JaegerExporter = "jaeger"
	NoOpExporter   = "noop"
	StdoutExporter = "stdout"
	OTLPExporter   = "otlp"
)

type JaegerConfig struct {
	Endpoint string
	User     string
	Password string
}

const (
	ModeGRPC = "grpc"
	ModeHTTP = "http"
)

type OTLPConfig struct {
	Mode string
}

type Config struct {
	ServiceName       string
	Version           string
	Exporter          string
	JaegerConfig      *JaegerConfig
	OTLPConfig        *OTLPConfig
	ApiMiddlewareName string
}

func Module(cfg Config) fx.Option {
	options := make([]fx.Option, 0)
	options = append(options,
		ProvideServiceName(func() string { return "ledger" }),
		ProvideServiceVersion(func() string { return cfg.Version }),
	)

	if cfg.Exporter == JaegerExporter && cfg.JaegerConfig != nil {
		if v := cfg.JaegerConfig.Endpoint; v != "" {
			options = append(options, ProvideJaegerCollectorEndpoint(func() jaeger.CollectorEndpointOption {
				return jaeger.WithEndpoint(v)
			}))
		}

		if v := cfg.JaegerConfig.User; v != "" {
			options = append(options, ProvideJaegerCollectorEndpoint(func() jaeger.CollectorEndpointOption {
				return jaeger.WithUsername(v)
			}))
		}

		if v := cfg.JaegerConfig.Password; v != "" {
			options = append(options, ProvideJaegerCollectorEndpoint(func() jaeger.CollectorEndpointOption {
				return jaeger.WithPassword(v)
			}))
		}
	}

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
	case StdoutExporter:
		options = append(options, StdoutModule())
	case JaegerExporter:
		options = append(options, JaegerModule())
	case NoOpExporter:
		options = append(options, NoOpModule())
	case OTLPExporter:
		options = append(options, OTLPModule())
		mode := ModeGRPC
		if cfg.OTLPConfig != nil && cfg.OTLPConfig.Mode != "" {
			mode = cfg.OTLPConfig.Mode
		}
		switch mode {
		case ModeGRPC:
			options = append(options, OTLPGRPCClientModule())
		case ModeHTTP:
			options = append(options, OTLPHTTPClientModule())
		}
	}
	if cfg.ApiMiddlewareName != "" {
		options = append(options, routes.ProvideGlobalMiddleware(func(tracerProvider trace.TracerProvider) gin.HandlerFunc {
			return otelgin.Middleware("ledger", otelgin.WithTracerProvider(tracerProvider))
		}))
	}
	return fx.Options(options...)
}
