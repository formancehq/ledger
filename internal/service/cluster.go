package service

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/systempb"
)

type Cluster interface {
	IsHealthy() bool
	GetLeader() uint64
}

type System interface {
	CreateLedger(ctx context.Context, req *systempb.CreateLedgerRequest) (*ledgerpb.LedgerInfo, error)
	DeleteLedger(ctx context.Context, name string) error
	GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error)
	GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error)
	ResolveLedger(ctx context.Context, ledgerName string) (string, uint64, error)
	ResolveLedgerLeader(ctx context.Context, ledgerName string) (uint64, error)
}