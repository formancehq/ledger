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

const (
	CommittedTransactions string = "COMMITTED_TRANSACTIONS"
	SavedMetadata         string = "SAVED_METADATA"
	UpdatedMapping        string = "UPDATED_MAPPING"
	RevertedTransaction   string = "REVERTED_TRANSACTION"
)

type ledgerMonitor struct {
	publisher *sharedpublish.TopicMapperPublisher
}

func (l *ledgerMonitor) publish(ctx context.Context, ledger string, et string, data interface{}) {
	err := l.publisher.Publish(ctx, et, baseEvent{
		Date:    time.Now(),
		Type:    et,
		Payload: data,
		Ledger:  ledger,
	})
	if err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
		return
	}
}

func (l *ledgerMonitor) CommittedTransactions(ctx context.Context, ledger string, result *ledger.CommitmentResult) {
	l.publish(ctx, ledger, CommittedTransactions, committedTransactions{
		Transactions:      result.GeneratedTransactions,
		Volumes:           result.PostCommitVolumes,
		PostCommitVolumes: result.PostCommitVolumes,
		PreCommitVolumes:  result.PreCommitVolumes,
	})
}

func (l ledgerMonitor) SavedMetadata(ctx context.Context, ledger string, targetType string, id string, metadata core.Metadata) {
	l.publish(ctx, ledger, SavedMetadata, savedMetadata{
		TargetType: targetType,
		TargetID:   id,
		Metadata:   metadata,
	})
}

func (l ledgerMonitor) UpdatedMapping(ctx context.Context, ledger string, mapping core.Mapping) {
	l.publish(ctx, ledger, UpdatedMapping, updatedMapping{
		Mapping: mapping,
	})
}

func (l ledgerMonitor) RevertedTransaction(ctx context.Context, ledger string, reverted core.Transaction, revert core.Transaction) {
	l.publish(ctx, ledger, RevertedTransaction, revertedTransaction{
		RevertedTransaction: reverted,
		RevertTransaction:   revert,
	})
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
