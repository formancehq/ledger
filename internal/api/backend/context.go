package backend

import (
	"context"
)

type ledgerKey struct{}

var _ledgerKey = ledgerKey{}

func ContextWithLedger(ctx context.Context, ledger Ledger) context.Context {
	return context.WithValue(ctx, _ledgerKey, ledger)
}

func LedgerFromContext(ctx context.Context) Ledger {
	return ctx.Value(_ledgerKey).(Ledger)
}
