package bus

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/publish"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/events"
)

type Monitor interface {
	CommittedTransactions(ctx context.Context, res ledger.Transaction, accountMetadata map[string]metadata.Metadata)
	SavedMetadata(ctx context.Context, targetType, id string, metadata metadata.Metadata)
	RevertedTransaction(ctx context.Context, reverted, revert *ledger.Transaction)
	DeletedMetadata(ctx context.Context, targetType string, targetID any, key string)
}

type noOpMonitor struct{}

func (n noOpMonitor) DeletedMetadata(ctx context.Context, targetType string, targetID any, key string) {
}

func (n noOpMonitor) CommittedTransactions(ctx context.Context, res ledger.Transaction, accountMetadata map[string]metadata.Metadata) {
}
func (n noOpMonitor) SavedMetadata(ctx context.Context, targetType string, id string, metadata metadata.Metadata) {
}
func (n noOpMonitor) RevertedTransaction(ctx context.Context, reverted, revert *ledger.Transaction) {
}

var _ Monitor = &noOpMonitor{}

func NewNoOpMonitor() *noOpMonitor {
	return &noOpMonitor{}
}

type ledgerMonitor struct {
	publisher  message.Publisher
	ledgerName string
}

var _ Monitor = &ledgerMonitor{}

func NewLedgerMonitor(publisher message.Publisher, ledgerName string) *ledgerMonitor {
	m := &ledgerMonitor{
		publisher:  publisher,
		ledgerName: ledgerName,
	}
	return m
}

func (l *ledgerMonitor) CommittedTransactions(ctx context.Context, txs ledger.Transaction, accountMetadata map[string]metadata.Metadata) {
	l.publish(ctx, events.EventTypeCommittedTransactions,
		newEventCommittedTransactions(CommittedTransactions{
			Ledger:          l.ledgerName,
			Transactions:    []ledger.Transaction{txs},
			AccountMetadata: accountMetadata,
		}))
}

func (l *ledgerMonitor) SavedMetadata(ctx context.Context, targetType, targetID string, metadata metadata.Metadata) {
	l.publish(ctx, events.EventTypeSavedMetadata,
		newEventSavedMetadata(SavedMetadata{
			Ledger:     l.ledgerName,
			TargetType: targetType,
			TargetID:   targetID,
			Metadata:   metadata,
		}))
}

func (l *ledgerMonitor) RevertedTransaction(ctx context.Context, reverted, revert *ledger.Transaction) {
	l.publish(ctx, events.EventTypeRevertedTransaction,
		newEventRevertedTransaction(RevertedTransaction{
			Ledger:              l.ledgerName,
			RevertedTransaction: *reverted,
			RevertTransaction:   *revert,
		}))
}

func (l *ledgerMonitor) DeletedMetadata(ctx context.Context, targetType string, targetID any, key string) {
	l.publish(ctx, events.EventTypeDeletedMetadata,
		newEventDeletedMetadata(DeletedMetadata{
			Ledger:     l.ledgerName,
			TargetType: targetType,
			TargetID:   targetID,
			Key:        key,
		}))
}

func (l *ledgerMonitor) publish(ctx context.Context, topic string, ev publish.EventMessage) {
	if err := l.publisher.Publish(topic, publish.NewMessage(ctx, ev)); err != nil {
		logging.FromContext(ctx).Errorf("publishing message: %s", err)
		return
	}
}
