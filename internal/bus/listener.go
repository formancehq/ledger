package bus

import (
	"context"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/publish"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/events"
)

type LedgerListener struct {
	publisher message.Publisher
}

var _ ledgercontroller.Listener = &LedgerListener{}

func NewLedgerListener(publisher message.Publisher) *LedgerListener {
	return &LedgerListener{
		publisher: publisher,
	}
}

func (lis *LedgerListener) CommittedTransactions(ctx context.Context, l string, txs ledger.Transaction, accountMetadata ledger.AccountMetadata) {
	lis.publish(ctx, events.EventTypeCommittedTransactions,
		newEventCommittedTransactions(CommittedTransactions{
			Ledger:          l,
			Transactions:    []ledger.Transaction{txs},
			AccountMetadata: accountMetadata,
		}))
}

func (lis *LedgerListener) SavedMetadata(ctx context.Context, l string, targetType, targetID string, metadata metadata.Metadata) {
	lis.publish(ctx, events.EventTypeSavedMetadata,
		newEventSavedMetadata(SavedMetadata{
			Ledger:     l,
			TargetType: targetType,
			TargetID:   targetID,
			Metadata:   metadata,
		}))
}

func (lis *LedgerListener) RevertedTransaction(ctx context.Context, l string, reverted, revert ledger.Transaction) {
	lis.publish(ctx, events.EventTypeRevertedTransaction,
		newEventRevertedTransaction(RevertedTransaction{
			Ledger:              l,
			RevertedTransaction: reverted,
			RevertTransaction:   revert,
		}))
}

func (lis *LedgerListener) DeletedMetadata(ctx context.Context, l string, targetType string, targetID any, key string) {
	lis.publish(ctx, events.EventTypeDeletedMetadata,
		newEventDeletedMetadata(DeletedMetadata{
			Ledger:     l,
			TargetType: targetType,
			TargetID:   targetID,
			Key:        key,
		}))
}

func (lis *LedgerListener) publish(ctx context.Context, topic string, ev publish.EventMessage) {
	msg := publish.NewMessage(ctx, ev)
	logging.FromContext(ctx).WithFields(map[string]any{
		"payload": string(msg.Payload),
		"topic":   topic,
	}).Debugf("send event %s", ev.Type)
	if err := lis.publisher.Publish(topic, msg); err != nil {
		logging.FromContext(ctx).Errorf("publishing message: %s", err)
		return
	}
}
