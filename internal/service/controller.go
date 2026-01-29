package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
)

type Controller interface {
	// Ledger management (read-only)
	GetAllLedgersInfo(ctx context.Context) (map[string]*commonpb.LedgerInfo, error)
	GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error)

	// Read operations
	GetTransaction(ctx context.Context, id uint32, transactionID uint64) (*commonpb.Transaction, error)
	GetAllLedgerLogs(ctx context.Context, id uint32, from uint64, to uint64) (store.Cursor[*commonpb.LedgerLog], error)
	GetAllLogs(ctx context.Context, from uint64, to uint64) (store.Cursor[*commonpb.Log], error)

	// Write operations - single entry point for all actions
	Apply(ctx context.Context, action *servicepb.Action) (*commonpb.Log, error)

	// Import/Export
	Import(ctx context.Context, id uint32, stream chan *commonpb.LedgerLog) error
	Export(ctx context.Context, id uint32, w ExportWriter) error
}

type ExportWriter interface {
	Write(ctx context.Context, log *commonpb.LedgerLog) error
}
