package ledger

import (
	"context"

	ledger "github.com/formancehq/ledger/internal"
)

type ExportWriter interface {
	Write(ctx context.Context, log ledger.Log) error
}

type ExportWriterFn func(ctx context.Context, log ledger.Log) error

func (fn ExportWriterFn) Write(ctx context.Context, log ledger.Log) error {
	return fn(ctx, log)
}
