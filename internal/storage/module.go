package storage

import (
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/system"
	"github.com/uptrace/bun"
	"go.uber.org/fx"
)

func NewFXModule(autoUpgrade bool) fx.Option {
	return fx.Options(
		driver.NewFXModule(autoUpgrade),
		fx.Provide(func(db *bun.DB) *system.Store {
			return system.New(db)
		}),
	)
}
