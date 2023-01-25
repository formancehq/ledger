package stripe

import (
	"context"

	"github.com/stripe/stripe-go/v72"
)

type Ingester interface {
	Ingest(ctx context.Context, batch []*stripe.BalanceTransaction, commitState TimelineState, tail bool) error
}

type IngesterFn func(ctx context.Context, batch []*stripe.BalanceTransaction,
	commitState TimelineState, tail bool) error

func (fn IngesterFn) Ingest(ctx context.Context, batch []*stripe.BalanceTransaction,
	commitState TimelineState, tail bool,
) error {
	return fn(ctx, batch, commitState, tail)
}
