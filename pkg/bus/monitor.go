package bus

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"time"
)

type eventType string

const (
	FallbackTopic                   = "NEW_EVENT"
	CommittedTransactions eventType = "COMMITTED_TRANSACTIONS"
	SavedMetadata         eventType = "SAVED_METADATA"
	UpdatedMapping        eventType = "UPDATED_MAPPING"
	RevertedTransaction   eventType = "REVERTED_TRANSACTION"
)

type ledgerMonitor struct {
	publisher message.Publisher
	topics    map[eventType]string
}

func (l *ledgerMonitor) publish(ctx context.Context, topic string, ledger string, et eventType, data interface{}) {
	err := l.publisher.Publish(topic, newMessage(ctx, baseEvent{
		Date:    time.Now(),
		Type:    et,
		Payload: data,
		Ledger:  ledger,
	}))
	if err != nil {
		sharedlogging.GetLogger(ctx).Errorf("Publishing message: %s", err)
		return
	}
}

func (l *ledgerMonitor) process(ctx context.Context, ledger string, event eventType, data interface{}) {
	topic, ok := l.topics[event]
	if ok {
		l.publish(ctx, topic, ledger, event, data)
		return
	}
	topic, ok = l.topics["*"]
	if ok {
		l.publish(ctx, topic, ledger, event, data)
		return
	}
	l.publish(ctx, FallbackTopic, ledger, event, data)
	return
}

func (l *ledgerMonitor) CommittedTransactions(ctx context.Context, ledger string, results []ledger.CommitTransactionResult) {
	l.process(ctx, ledger, CommittedTransactions, committedTransactions{
		Transactions: results,
	})
}

func (l ledgerMonitor) SavedMetadata(ctx context.Context, ledger string, targetType string, id string, metadata core.Metadata) {
	l.process(ctx, ledger, SavedMetadata, savedMetadata{
		TargetType: targetType,
		TargetID:   id,
		Metadata:   metadata,
	})
}

func (l ledgerMonitor) UpdatedMapping(ctx context.Context, ledger string, mapping core.Mapping) {
	l.process(ctx, ledger, UpdatedMapping, updatedMapping{
		Mapping: mapping,
	})
}

func (l ledgerMonitor) RevertedTransaction(ctx context.Context, ledger string, reverted core.Transaction, revert core.Transaction) {
	l.process(ctx, ledger, RevertedTransaction, revertedTransaction{
		RevertedTransaction: reverted,
		RevertTransaction:   revert,
	})
}

var _ ledger.Monitor = &ledgerMonitor{}

type Option func(monitor *ledgerMonitor)

func WithGlobalTopic(v string) Option {
	return func(monitor *ledgerMonitor) {
		monitor.topics["*"] = v
	}
}

func WithTopic(key eventType, v string) Option {
	return func(monitor *ledgerMonitor) {
		monitor.topics[key] = v
	}
}

func NewLedgerMonitor(publisher message.Publisher, opts ...Option) *ledgerMonitor {
	m := &ledgerMonitor{
		publisher: publisher,
		topics:    map[eventType]string{},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}
