//go:build pyroscope

package pyroscope

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/grafana/pyroscope-go"
	"go.uber.org/fx"
)

// Module returns an fx.Option that provides Pyroscope profiling.
func Module(cfg Config) fx.Option {
	if !cfg.Enabled {
		return fx.Options()
	}

	return fx.Options(
		fx.Provide(func() Config {
			return cfg
		}),
		fx.Invoke(startProfiler),
	)
}

// startProfiler starts the Pyroscope profiler with the given configuration.
func startProfiler(lc fx.Lifecycle, cfg Config, logger logging.Logger) error {
	// Setup runtime profiling rates
	cfg.SetupRuntimeProfiling()

	pyroscopeCfg := cfg.PyroscopeConfig()
	pyroscopeCfg.Logger = &pyroscopeLogger{logger: logger}

	var profiler *pyroscope.Profiler

	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logger.WithFields(map[string]any{
				"server_address":   cfg.ServerAddress,
				"application_name": cfg.ApplicationName,
				"profile_types":    cfg.ProfileTypes,
				"tags":             cfg.Tags,
			}).Infof("Starting Pyroscope profiler")

			var err error
			profiler, err = pyroscope.Start(pyroscopeCfg)
			if err != nil {
				return fmt.Errorf("starting pyroscope profiler: %w", err)
			}

			return nil
		},
		OnStop: func(ctx context.Context) error {
			if profiler != nil {
				logger.Infof("Stopping Pyroscope profiler")
				if err := profiler.Stop(); err != nil {
					logger.WithField("error", err).Infof("Error stopping Pyroscope profiler")
				}
			}
			return nil
		},
	})

	return nil
}

// pyroscopeLogger adapts the logging.Logger interface to pyroscope.Logger.
type pyroscopeLogger struct {
	logger logging.Logger
}

func (l *pyroscopeLogger) Infof(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

func (l *pyroscopeLogger) Debugf(format string, args ...interface{}) {
	l.logger.Debugf(format, args...)
}

func (l *pyroscopeLogger) Errorf(format string, args ...interface{}) {
	l.logger.Errorf(format, args...)
}
