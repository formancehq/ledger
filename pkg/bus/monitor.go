package bus

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/query"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/publish"
)

type ledgerMonitor struct {
	publisher  message.Publisher
	ledgerName string
}

var _ query.Monitor = &ledgerMonitor{}

func NewLedgerMonitor(publisher message.Publisher, ledgerName string) *ledgerMonitor {
	m := &ledgerMonitor{
		publisher:  publisher,
		ledgerName: ledgerName,
	}
	return m
}

func (l *ledgerMonitor) CommittedTransactions(ctx context.Context, txs ...core.Transaction) {
	l.publish(ctx, EventTypeCommittedTransactions,
		newEventCommittedTransactions(CommittedTransactions{
			Ledger:       l.ledgerName,
			Transactions: txs,
		}))
}

func (l *ledgerMonitor) SavedMetadata(ctx context.Context, targetType, targetID string, metadata metadata.Metadata) {
	l.publish(ctx, EventTypeSavedMetadata,
		newEventSavedMetadata(SavedMetadata{
			Ledger:     l.ledgerName,
			TargetType: targetType,
			TargetID:   targetID,
			Metadata:   metadata,
		}))
}

func (l *ledgerMonitor) RevertedTransaction(ctx context.Context, reverted, revert *core.Transaction) {
	l.publish(ctx, EventTypeRevertedTransaction,
		newEventRevertedTransaction(RevertedTransaction{
			Ledger:              l.ledgerName,
			RevertedTransaction: *reverted,
			RevertTransaction:   *revert,
		}))
}

func (l *ledgerMonitor) publish(ctx context.Context, topic string, ev EventMessage) {
	if err := l.publisher.Publish(topic, publish.NewMessage(ctx, ev)); err != nil {
		logging.FromContext(ctx).Errorf("publishing message: %s", err)
		return
	}
}
