package bus

import (
	"github.com/formancehq/ledger/internal/controller/ledger"
	"go.uber.org/fx"
)

func NewFxModule() fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotate(NewLedgerListener, fx.As(new(ledger.Listener)))),
	)
}
