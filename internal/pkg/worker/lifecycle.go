package worker

import (
	"context"

	"go.uber.org/fx"
)

// Lifecycle is the interface for components with a simple Start/Stop lifecycle.
type Lifecycle interface {
	Start()
	Stop()
}

// FxHook returns an fx.Hook that starts and stops a Lifecycle component.
// It eliminates the boilerplate of wrapping Start/Stop in OnStart/OnStop closures.
func FxHook(w Lifecycle) fx.Hook {
	return fx.Hook{
		OnStart: func(_ context.Context) error {
			w.Start()

			return nil
		},
		OnStop: func(_ context.Context) error {
			w.Stop()

			return nil
		},
	}
}
