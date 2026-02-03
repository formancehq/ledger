package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
)

type Controller interface {
	// Ledger management (read-only)
	GetAllLedgersInfo(ctx context.Context) (store.Cursor[*commonpb.LedgerInfo], error)
	GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error)

	// Read operations
	GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error)
	GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error)
	GetAllLogs(ctx context.Context, from uint64, to uint64) (store.Cursor[*commonpb.Log], error)

	// Write operations - single entry point for all actions
	Apply(ctx context.Context, actions ...*servicepb.Request) ([]*commonpb.Log, error)

	// Import/Export
	Import(ctx context.Context, ledgerName string, stream chan *commonpb.LedgerLog) error
	Export(ctx context.Context, ledgerName string, w ExportWriter) error
}

type ExportWriter interface {
	Write(ctx context.Context, log *commonpb.LedgerLog) error
}
