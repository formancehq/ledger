package bus

import (
	"context"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/publish"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"go.uber.org/fx"
)

type ledgerMonitor struct {
	publisher *publish.TopicMapperPublisher
}

var _ ledger.Monitor = &ledgerMonitor{}

func newLedgerMonitor(publisher *publish.TopicMapperPublisher) *ledgerMonitor {
	m := &ledgerMonitor{
		publisher: publisher,
	}
	return m
}

func LedgerMonitorModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(
				newLedgerMonitor,
				fx.ParamTags(``, `group:"monitorOptions"`),
			),
		),
		ledger.ProvideResolverOption(func(monitor *ledgerMonitor) ledger.ResolveOptionFn {
			return ledger.WithMonitor(monitor)
		}),
	)
}

func (l *ledgerMonitor) CommittedTransactions(ctx context.Context, ledger string, txs ...core.ExpandedTransaction) {
	postCommitVolumes := core.AggregatePostCommitVolumes(txs...)
	l.publish(ctx, EventTypeCommittedTransactions,
		newEventCommittedTransactions(CommittedTransactions{
			Ledger:            ledger,
			Transactions:      txs,
			Volumes:           postCommitVolumes,
			PostCommitVolumes: postCommitVolumes,
			PreCommitVolumes:  core.AggregatePreCommitVolumes(txs...),
		}))
}

func (l *ledgerMonitor) SavedMetadata(ctx context.Context, ledger, targetType, targetID string, metadata core.Metadata) {
	l.publish(ctx, EventTypeSavedMetadata,
		newEventSavedMetadata(SavedMetadata{
			Ledger:     ledger,
			TargetType: targetType,
			TargetID:   targetID,
			Metadata:   metadata,
		}))
}

func (l *ledgerMonitor) UpdatedMapping(ctx context.Context, ledger string, mapping core.Mapping) {
	l.publish(ctx, EventTypeUpdatedMapping,
		newEventUpdatedMapping(UpdatedMapping{
			Ledger:  ledger,
			Mapping: mapping,
		}))
}

func (l *ledgerMonitor) RevertedTransaction(ctx context.Context, ledger string, reverted, revert *core.ExpandedTransaction) {
	l.publish(ctx, EventTypeRevertedTransaction,
		newEventRevertedTransaction(RevertedTransaction{
			Ledger:              ledger,
			RevertedTransaction: *reverted,
			RevertTransaction:   *revert,
		}))
}

func (l *ledgerMonitor) publish(ctx context.Context, topic string, ev EventMessage) {
	if err := l.publisher.Publish(ctx, topic, ev); err != nil {
		logging.GetLogger(ctx).Errorf("publishing message: %s", err)
		return
	}
}
