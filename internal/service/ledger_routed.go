package service

import (
	"context"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"go.uber.org/zap"
)

// RoutedLedger routes requests to the leader, either directly or via gRPC
type RoutedLedger struct {
	ledger *LeaderRouted[Ledger]
	logger *zap.Logger
}

// NewRoutedLedger creates a new routed ledger service
func NewRoutedLedger(cluster ClusterClient, defaultLedger Ledger, logger *zap.Logger) *RoutedLedger {
	grpcLedgerImpl := newGRPCLedger(cluster, logger)
	var grpcLedger Ledger = grpcLedgerImpl
	ledgerRouter := NewLeaderRouted(cluster.GetRaft(), defaultLedger, grpcLedger)

	return &RoutedLedger{
		ledger: ledgerRouter,
		logger: logger,
	}
}

// CreateTransaction creates a new transaction, routing to leader if needed
func (r *RoutedLedger) CreateTransaction(ctx context.Context, ledgerName string, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	ledgerImpl := r.ledger.Get(ctx)
	return ledgerImpl.CreateTransaction(ctx, ledgerName, parameters)
}
// Stub methods for other Ledger interface methods
func (r *RoutedLedger) RevertTransaction(ctx context.Context, ledgerName string, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	ledgerImpl := r.ledger.Get(ctx)
	return ledgerImpl.RevertTransaction(ctx, ledgerName, parameters)
}

func (r *RoutedLedger) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	ledgerImpl := r.ledger.Get(ctx)
	return ledgerImpl.SaveTransactionMetadata(ctx, ledgerName, parameters)
}

func (r *RoutedLedger) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	ledgerImpl := r.ledger.Get(ctx)
	return ledgerImpl.SaveAccountMetadata(ctx, ledgerName, parameters)
}

func (r *RoutedLedger) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	ledgerImpl := r.ledger.Get(ctx)
	return ledgerImpl.DeleteTransactionMetadata(ctx, ledgerName, parameters)
}

func (r *RoutedLedger) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	ledgerImpl := r.ledger.Get(ctx)
	return ledgerImpl.DeleteAccountMetadata(ctx, ledgerName, parameters)
}

func (r *RoutedLedger) Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error {
	ledgerImpl := r.ledger.Get(ctx)
	return ledgerImpl.Import(ctx, ledgerName, stream)
}

func (r *RoutedLedger) Export(ctx context.Context, ledgerName string, w ExportWriter) error {
	ledgerImpl := r.ledger.Get(ctx)
	return ledgerImpl.Export(ctx, ledgerName, w)
}
