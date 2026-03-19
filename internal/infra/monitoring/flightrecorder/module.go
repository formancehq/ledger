package flightrecorder

import (
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v4/logging"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
)

// Module returns an fx.Option that provides the flight recorder.
func Module(cfg Config) fx.Option {
	if !cfg.Enabled {
		return fx.Options()
	}

	return fx.Options(
		fx.Provide(func(logger logging.Logger) *Recorder {
			return New(cfg, logger.WithFields(map[string]any{
				"cmp": "flight-recorder",
			}))
		}),
		fx.Invoke(func(lc fx.Lifecycle, recorder *Recorder) {
			lc.Append(worker.FxHook(recorder))
		}),
	)
}
