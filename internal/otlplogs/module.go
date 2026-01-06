package otlplogs

import (
	"io"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/otlp"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

const (
	logsProviderOptionKey = `group:"_logsProviderOption"`
	logsRuntimeOptionKey  = `group:"_logsRuntimeOption"`

	StdoutExporter = "stdout"
	OTLPExporter   = "otlp"
)

type ModuleConfig struct {
	Exporter   string
	OTLPConfig *OTLPConfig
	Output     io.Writer
	Debug      bool
	FormatJSON bool
	Fields     map[string]any
}

type OTLPConfig struct {
	Mode     string
	Endpoint string
	Insecure bool
}

func ProvideLogsProviderOption(v any, annotations ...fx.Annotation) fx.Option {
	annotations = append(annotations, fx.ResultTags(logsProviderOptionKey))
	return fx.Provide(fx.Annotate(v, annotations...))
}

func LogsModule(cfg ModuleConfig) fx.Option {

	options := make([]fx.Option, 0)
	options = append(options,
		fx.Supply(cfg),
		fx.Provide(fx.Annotate(func(exporter sdklog.Exporter, options ...sdklog.LoggerProviderOption) log.LoggerProvider {
			loggerProvider := sdklog.NewLoggerProvider(
				append(options, sdklog.WithProcessor(
					sdklog.NewBatchProcessor(exporter),
				))...,
			)

			global.SetLoggerProvider(loggerProvider)

			return loggerProvider
		}, fx.ParamTags(``, logsProviderOptionKey))),
		ProvideLogsProviderOption(sdklog.WithResource),
		fx.Decorate(func(_ logging.Logger, loggerProvider log.LoggerProvider) (logging.Logger, error) {
			l := logrus.New()
			l.WithFields(cfg.Fields)
			l.AddHook(&otelLogrusHook{
				Logger: loggerProvider.Logger("root"),
			})
			l.SetOutput(cfg.Output)
			if cfg.Debug {
				l.Level = logrus.DebugLevel
			}

			var formatter logrus.Formatter
			if cfg.FormatJSON {
				jsonFormatter := &logrus.JSONFormatter{}
				formatter = jsonFormatter
			} else {
				textFormatter := new(logrus.TextFormatter)
				textFormatter.FullTimestamp = true
				formatter = textFormatter
			}

			l.SetFormatter(formatter)

			return logging.NewLogrus(l), nil
		}),
	)

	switch cfg.Exporter {
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
					options = append(options, ProvideOTLPLogsGRPCOption(func() otlploggrpc.Option {
						return otlploggrpc.WithEndpoint(cfg.OTLPConfig.Endpoint)
					}))
				}
				if cfg.OTLPConfig.Insecure {
					options = append(options, ProvideOTLPLogsGRPCOption(func() otlploggrpc.Option {
						return otlploggrpc.WithInsecure()
					}))
				}
			}

			options = append(options, ProvideOTLPLogsGRPCExporter())
		}
	default:
		options = append(options, fx.Provide(fx.Annotate(NewNoOpExporter, fx.As(new(sdklog.Exporter)))))
	}

	return fx.Options(options...)
}
