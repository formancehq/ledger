package opentelemetrymetrics

import (
	"github.com/numary/ledger/pkg/opentelemetry"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.uber.org/fx"
)

type OTLPMetricsConfig struct {
	Mode     string
	Endpoint string
	Insecure bool
}

type MetricsModuleConfig struct {
	Exporter   string
	OTLPConfig *OTLPMetricsConfig
}

func MetricsModule(cfg MetricsModuleConfig) fx.Option {
	options := make([]fx.Option, 0)
	switch cfg.Exporter {
	case OTLPMetricsExporter:
		options = append(options, OTLPMeterModule())
		mode := opentelemetry.ModeGRPC
		if cfg.OTLPConfig != nil {
			if cfg.OTLPConfig.Mode != "" {
				mode = cfg.OTLPConfig.Mode
			}
			switch mode {
			case opentelemetry.ModeGRPC:
				if cfg.OTLPConfig.Endpoint != "" {
					options = append(options, ProvideOTLPMeterGRPCClientOption(func() otlpmetricgrpc.Option {
						return otlpmetricgrpc.WithEndpoint(cfg.OTLPConfig.Endpoint)
					}))
				}
				if cfg.OTLPConfig.Insecure {
					options = append(options, ProvideOTLPMeterGRPCClientOption(func() otlpmetricgrpc.Option {
						return otlpmetricgrpc.WithInsecure()
					}))
				}
			case opentelemetry.ModeHTTP:
				if cfg.OTLPConfig.Endpoint != "" {
					options = append(options, ProvideOTLPMeterHTTPClientOption(func() otlpmetrichttp.Option {
						return otlpmetrichttp.WithEndpoint(cfg.OTLPConfig.Endpoint)
					}))
				}
				if cfg.OTLPConfig.Insecure {
					options = append(options, ProvideOTLPMeterHTTPClientOption(func() otlpmetrichttp.Option {
						return otlpmetrichttp.WithInsecure()
					}))
				}
			}
		}
		switch mode {
		case opentelemetry.ModeGRPC:
			options = append(options, OTLPMeterGRPCClientModule())
		case opentelemetry.ModeHTTP:
			options = append(options, OTLPMeterHTTPClientModule())
		}
	case NoOpMetricsExporter:
		options = append(options, NoOpMeterModule())
	}
	return fx.Options(options...)
}
