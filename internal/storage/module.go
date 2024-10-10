package storage

import (
	"github.com/formancehq/ledger/internal/storage/driver"
	"go.uber.org/fx"
)

func NewFXModule(autoUpgrade bool) fx.Option {
	return fx.Options(
		driver.NewFXModule(autoUpgrade),
	)
}
