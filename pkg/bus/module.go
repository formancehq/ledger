package bus

import (
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"go.uber.org/fx"
)

func LedgerMonitorModule() fx.Option {
	return fx.Decorate(fx.Annotate(newLedgerMonitor, fx.As(new(monitor.Monitor))))
}
