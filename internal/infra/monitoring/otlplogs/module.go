package otlplogs

import (
	"context"
	"fmt"
	"io"
	"runtime/debug"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/otlp"
)

const (
	OTLPExporter = "otlp"
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

		if mode == otlp.ModeGRPC {
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

	// Build the console core (writes to cfg.Output).
	level := zapcore.InfoLevel
	if cfg.Debug {
		level = zapcore.DebugLevel
	}

	var encoder zapcore.Encoder

	if cfg.FormatJSON {
		encoderCfg := zap.NewProductionEncoderConfig()
		encoderCfg.EncodeTime = zapcore.TimeEncoderOfLayout(time.RFC3339Nano)
		encoder = zapcore.NewJSONEncoder(encoderCfg)
	} else {
		encoderCfg := zap.NewDevelopmentEncoderConfig()
		encoderCfg.EncodeTime = zapcore.TimeEncoderOfLayout(time.RFC3339Nano)
		encoder = zapcore.NewConsoleEncoder(encoderCfg)
	}

	consoleCore := zapcore.NewCore(encoder, zapcore.AddSync(cfg.Output), level)

	// Build the OTel bridge core.
	otelCore := otelzap.NewCore("root", otelzap.WithLoggerProvider(loggerProvider))

	// Combine both cores.
	core := zapcore.NewTee(consoleCore, otelCore)
	l := zap.New(core)

	ret := NewZapLogger(l.Sugar())
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
			err := loggerProvider.ForceFlush(context.Background())
			if err != nil {
				logger.Errorf("Failed to flush logs: %v", err)
			}

			err = loggerProvider.Shutdown(context.Background())
			if err != nil {
				logger.Errorf("Failed to shutdown logs: %v", err)
			}
		default:
			logger.Errorf("Unknown logger provider type: %T", loggerProvider)
		}

		panic(e)
	}
}

func Go(f func(), logger logging.Logger) {
	go func() {
		defer RecoverAndLogPanics(logger)

		f()
	}()
}

// GoWait launches f in a goroutine with panic recovery and returns a function
// that blocks until the goroutine has exited. Use this instead of Go when the
// caller must guarantee the goroutine is fully done (e.g. in OnStop hooks).
func GoWait(f func(), logger logging.Logger) func() {
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer RecoverAndLogPanics(logger)

		f()
	}()

	return func() { <-done }
}
