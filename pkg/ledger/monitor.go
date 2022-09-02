package ledger

import (
	"context"

	"github.com/numary/ledger/pkg/core"
)

type Monitor interface {
	CommittedTransactions(ctx context.Context, ledger string, res ...core.ExpandedTransaction)
	SavedMetadata(ctx context.Context, ledger, targetType, id string, metadata core.Metadata)
	UpdatedMapping(ctx context.Context, ledger string, mapping core.Mapping)
	RevertedTransaction(ctx context.Context, ledger string, reverted, revert *core.ExpandedTransaction)
}

type noOpMonitor struct{}

var _ Monitor = &noOpMonitor{}

func (n noOpMonitor) CommittedTransactions(context.Context, string, ...core.ExpandedTransaction) {}
func (n noOpMonitor) SavedMetadata(context.Context, string, string, string, core.Metadata)       {}
func (n noOpMonitor) UpdatedMapping(context.Context, string, core.Mapping)                       {}
func (n noOpMonitor) RevertedTransaction(context.Context, string, *core.ExpandedTransaction, *core.ExpandedTransaction) {
}
