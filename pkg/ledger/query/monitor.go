package query

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type Monitor interface {
	CommittedTransactions(ctx context.Context, res ...core.Transaction)
	SavedMetadata(ctx context.Context, targetType, id string, metadata metadata.Metadata)
	RevertedTransaction(ctx context.Context, reverted, revert *core.Transaction)
}

type noOpMonitor struct{}

func (n noOpMonitor) CommittedTransactions(ctx context.Context, res ...core.Transaction) {
}
func (n noOpMonitor) SavedMetadata(ctx context.Context, targetType string, id string, metadata metadata.Metadata) {
}
func (n noOpMonitor) RevertedTransaction(ctx context.Context, reverted, revert *core.Transaction) {
}

var _ Monitor = &noOpMonitor{}

func NewNoOpMonitor() *noOpMonitor {
	return &noOpMonitor{}
}
