package otlplogs

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime/debug"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
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

func Logger(cfg ModuleConfig) (logging.Logger, error) {

	var (
		exporter sdklog.Exporter
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
			options := make([]otlploggrpc.Option, 0)
			if cfg.OTLPConfig != nil {
				if cfg.OTLPConfig.Endpoint != "" {
					options = append(options, otlploggrpc.WithEndpoint(cfg.OTLPConfig.Endpoint))
				}
				if cfg.OTLPConfig.Insecure {
					options = append(options, otlploggrpc.WithInsecure())
				}
			}

			var err error
			exporter, err = otlploggrpc.New(context.Background(), options...)
			if err != nil {
				return nil, err
			}
		}
	default:
		exporter = NewNoOpExporter()
	}

	// todo: make go libs export resource attributes without the fx wrapper
	defaultResource := resource.Default()
	attributes := make([]attribute.KeyValue, 0)
	attributes = append(attributes, attribute.String("service.name", "ledger-exp"))
	attributes = append(attributes, attribute.String("service.version", "0.1.0"))
	resource, err := resource.Merge(defaultResource, resource.NewSchemaless(attributes...))
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	loggerProvider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(
			sdklog.NewBatchProcessor(exporter),
		),
		sdklog.WithResource(resource),
	)

	global.SetLoggerProvider(loggerProvider)

	l := logrus.New()
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
	ret := logging.NewLogrus(l)
	logger := ret.WithFields(cfg.Fields)

	return logger, nil
}

func RecoverAndLogPanics(logger logging.Logger) {
	if e := recover(); e != nil {
		logger.Errorf("Panicked: %v", e)
		_, err := logger.Writer().Write(debug.Stack())
		if err != nil {
			logger.Errorf("Failed to write stack trace: %v", err)
		}

		switch loggerProvider := global.GetLoggerProvider().(type) {
		case *sdklog.LoggerProvider:
			if err := loggerProvider.ForceFlush(context.Background()); err != nil {
				logger.Errorf("Failed to flush logs: %v", err)
			}
			if err := loggerProvider.Shutdown(context.Background()); err != nil {
				logger.Errorf("Failed to shutdown logs: %v", err)
			}
		default:
			logger.Errorf("Unknown logger provider type: %T", loggerProvider)
		}

		os.Exit(28) // Why 28? I don't know...
	}
}

func Go(f func(), logger logging.Logger) {
	go func() {
		defer RecoverAndLogPanics(logger)
		f()
	}()
}
