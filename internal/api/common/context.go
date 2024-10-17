package common

import (
	"context"

	"github.com/formancehq/ledger/internal/controller/ledger"
)

type ledgerKey struct{}

var _ledgerKey = ledgerKey{}

func ContextWithLedger(ctx context.Context, ledger ledger.Controller) context.Context {
	return context.WithValue(ctx, _ledgerKey, ledger)
}

func LedgerFromContext(ctx context.Context) ledger.Controller {
	return ctx.Value(_ledgerKey).(ledger.Controller)
}
