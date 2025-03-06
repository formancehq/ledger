package drivers

import (
	"github.com/formancehq/ledger/internal/storage/system"
	"go.uber.org/fx"
)

// NewFXModule create a new fx module
func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(func(store *system.DefaultStore) Store {
			return store
		}),
		fx.Provide(NewRegistry),
	)
}
