package bus

import (
	"context"

	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedpublish"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"go.uber.org/fx"
)

type ledgerMonitor struct {
	publisher *sharedpublish.TopicMapperPublisher
}

var _ ledger.Monitor = &ledgerMonitor{}

func NewLedgerMonitor(publisher *sharedpublish.TopicMapperPublisher) *ledgerMonitor {
	m := &ledgerMonitor{
		publisher: publisher,
	}
	return m
}

func LedgerMonitorModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(
				NewLedgerMonitor,
				fx.ParamTags(``, `group:"monitorOptions"`),
			),
		),
		ledger.ProvideResolverOption(func(monitor *ledgerMonitor) ledger.ResolveOptionFn {
			return ledger.WithMonitor(monitor)
		}),
	)
}

func (l *ledgerMonitor) CommittedTransactions(ctx context.Context, ledger string, res *ledger.CommitResult) {
	if err := l.publisher.Publish(ctx, EventLedgerCommittedTransactions,
		NewEventLedgerCommittedTransactions(CommittedTransactions{
			Transactions:      res.GeneratedTransactions,
			Volumes:           res.PostCommitVolumes,
			PostCommitVolumes: res.PostCommitVolumes,
			PreCommitVolumes:  res.PreCommitVolumes,
		}, ledger),
	); err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
	}
}

func (l *ledgerMonitor) SavedMetadata(ctx context.Context, ledger, targetType, targetID string, metadata core.Metadata) {
	if err := l.publisher.Publish(ctx, EventLedgerSavedMetadata,
		NewEventLedgerSavedMetadata(SavedMetadata{
			TargetType: targetType,
			TargetID:   targetID,
			Metadata:   metadata,
		}, ledger),
	); err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
	}
}

func (l *ledgerMonitor) UpdatedMapping(ctx context.Context, ledger string, mapping core.Mapping) {
	if err := l.publisher.Publish(ctx, EventLedgerUpdatedMapping,
		NewEventLedgerUpdatedMapping(UpdatedMapping{
			Mapping: mapping,
		}, ledger),
	); err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
	}
}

func (l *ledgerMonitor) RevertedTransaction(ctx context.Context, ledger string, reverted, revert *core.ExpandedTransaction) {
	if err := l.publisher.Publish(ctx, EventLedgerRevertedTransaction,
		NewEventLedgerRevertedTransaction(RevertedTransaction{
			RevertedTransaction: *reverted,
			RevertTransaction:   *revert,
		}, ledger),
	); err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
	}
}
