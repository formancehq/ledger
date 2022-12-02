package ledger

import (
	"context"

	"github.com/numary/ledger/pkg/core"
)

type Monitor interface {
	CommittedTransactions(ctx context.Context, ledger string, res CommitResult)
	SavedMetadata(ctx context.Context, ledger, targetType, id string, metadata core.Metadata)
	UpdatedMapping(ctx context.Context, ledger string, mapping core.Mapping)
	RevertedTransaction(ctx context.Context, ledger string, reverted, revert *core.ExpandedTransaction)
}

type noOpMonitor struct{}

func (n noOpMonitor) CommittedTransactions(ctx context.Context, s string, result CommitResult) {}
func (n noOpMonitor) SavedMetadata(ctx context.Context, ledger string, targetType string, id string, metadata core.Metadata) {
}
func (n noOpMonitor) UpdatedMapping(ctx context.Context, s string, mapping core.Mapping) {}
func (n noOpMonitor) RevertedTransaction(ctx context.Context, ledger string, reverted, revert *core.ExpandedTransaction) {
}

var _ Monitor = &noOpMonitor{}

func NewNoOpMonitor() *noOpMonitor {
	return &noOpMonitor{}
}
