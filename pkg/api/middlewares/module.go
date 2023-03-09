package middlewares

import (
	"github.com/formancehq/ledger/pkg/ledger"
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewLedgerMiddleware),
	fx.Provide(func() ledger.Locker {
		return ledger.NewInMemoryLocker()
	}),
)
