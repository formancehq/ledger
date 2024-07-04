package engine

import (
	"context"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"
)

type ExportWriter interface {
	Write(ctx context.Context, log *ledger.ChainedLog) error
}

type ExportWriterFn func(ctx context.Context, log *ledger.ChainedLog) error

func (fn ExportWriterFn) Write(ctx context.Context, log *ledger.ChainedLog) error {
	return fn(ctx, log)
}

func (l *Ledger) Export(ctx context.Context, w ExportWriter) error {
	return bunpaginate.Iterate(
		ctx,
		ledgerstore.
			NewGetLogsQuery(ledgerstore.NewPaginatedQueryOptions[any](nil).WithPageSize(100)).
			WithOrder(bunpaginate.OrderAsc),
		func(ctx context.Context, q ledgerstore.GetLogsQuery) (*bunpaginate.Cursor[ledger.ChainedLog], error) {
			return l.store.GetLogs(ctx, q)
		},
		func(cursor *bunpaginate.Cursor[ledger.ChainedLog]) error {
			for _, data := range cursor.Data {
				if err := w.Write(ctx, &data); err != nil {
					return err
				}
			}
			return nil
		},
	)
}
