package bus

import (
	"context"
	"time"

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
	publish(ctx, l, ledger, EventLedgerCommittedTransactions,
		CommittedTransactions{
			Transactions:      res.GeneratedTransactions,
			Volumes:           res.PostCommitVolumes,
			PostCommitVolumes: res.PostCommitVolumes,
			PreCommitVolumes:  res.PreCommitVolumes,
		})
}

func (l *ledgerMonitor) SavedMetadata(ctx context.Context, ledger, targetType, targetID string, metadata core.Metadata) {
	publish(ctx, l, ledger, EventLedgerSavedMetadata,
		SavedMetadata{
			TargetType: targetType,
			TargetID:   targetID,
			Metadata:   metadata,
		})
}

func (l *ledgerMonitor) UpdatedMapping(ctx context.Context, ledger string, mapping core.Mapping) {
	publish(ctx, l, ledger, EventLedgerUpdatedMapping,
		UpdatedMapping{
			Mapping: mapping,
		})
}

func (l *ledgerMonitor) RevertedTransaction(ctx context.Context, ledger string, reverted, revert *core.ExpandedTransaction) {
	publish(ctx, l, ledger, EventLedgerRevertedTransaction,
		RevertedTransaction{
			RevertedTransaction: *reverted,
			RevertTransaction:   *revert,
		})
}

func publish[T any](ctx context.Context, l *ledgerMonitor, ledger, eventType string, payload T) {
	if err := l.publisher.Publish(ctx, eventType,
		EventLedgerMessage[T]{
			Date:    time.Now().UTC(),
			Type:    eventType,
			Payload: payload,
			Ledger:  ledger,
		}); err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
	}
}
