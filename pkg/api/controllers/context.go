package controllers

import (
	"context"

	"github.com/formancehq/ledger/pkg/ledger"
)

type ledgerKey struct{}

var _ledgerKey = ledgerKey{}

func ContextWithLedger(ctx context.Context, ledger *ledger.Ledger) context.Context {
	return context.WithValue(ctx, _ledgerKey, ledger)
}

func LedgerFromContext(ctx context.Context) *ledger.Ledger {
	return ctx.Value(_ledgerKey).(*ledger.Ledger)
}
