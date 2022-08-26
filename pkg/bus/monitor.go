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
	if err := l.publisher.Publish(ctx, core.EventLedgerTypeCommittedTransactions,
		core.NewEventLedgerCommittedTransactions(core.CommittedTransactions{
			Ledger:            ledger,
			Transactions:      res.GeneratedTransactions,
			Volumes:           res.PostCommitVolumes,
			PostCommitVolumes: res.PostCommitVolumes,
			PreCommitVolumes:  res.PreCommitVolumes,
		}),
	); err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
	}
}

func (l *ledgerMonitor) SavedMetadata(ctx context.Context, ledger, targetType, targetID string, metadata core.Metadata) {
	if err := l.publisher.Publish(ctx, core.EventLedgerTypeSavedMetadata,
		core.NewEventLedgerSavedMetadata(core.SavedMetadata{
			Ledger:     ledger,
			TargetType: targetType,
			TargetID:   targetID,
			Metadata:   metadata,
		}),
	); err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
	}
}

func (l *ledgerMonitor) UpdatedMapping(ctx context.Context, ledger string, mapping core.Mapping) {
	if err := l.publisher.Publish(ctx, core.EventLedgerTypeUpdatedMapping,
		core.NewEventLedgerUpdatedMapping(core.UpdatedMapping{
			Ledger:  ledger,
			Mapping: mapping,
		}),
	); err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
	}
}

func (l *ledgerMonitor) RevertedTransaction(ctx context.Context, ledger string, reverted, revert *core.ExpandedTransaction) {
	if err := l.publisher.Publish(ctx, core.EventLedgerTypeRevertedTransaction,
		core.NewEventLedgerRevertedTransaction(core.RevertedTransaction{
			Ledger:              ledger,
			RevertedTransaction: *reverted,
			RevertTransaction:   *revert,
		}),
	); err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
	}
}
