package v1

import (
	"context"

	"github.com/formancehq/ledger/internal/api/backend"
)

type ledgerKey struct{}

var _ledgerKey = ledgerKey{}

func ContextWithLedger(ctx context.Context, ledger backend.Ledger) context.Context {
	return context.WithValue(ctx, _ledgerKey, ledger)
}

func LedgerFromContext(ctx context.Context) backend.Ledger {
	return ctx.Value(_ledgerKey).(backend.Ledger)
}
