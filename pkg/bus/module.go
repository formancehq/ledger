package bus

import (
	"github.com/formancehq/ledger/pkg/ledger/query"
	"go.uber.org/fx"
)

func LedgerMonitorModule() fx.Option {
	return fx.Decorate(fx.Annotate(newLedgerMonitor, fx.As(new(query.Monitor))))
}
