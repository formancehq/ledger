package bus

import (
	"context"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"go.uber.org/fx"
	"time"
)

const (
	FallbackTopic                = "NEW_EVENT"
	CommittedTransactions string = "COMMITTED_TRANSACTIONS"
	SavedMetadata         string = "SAVED_METADATA"
	UpdatedMapping        string = "UPDATED_MAPPING"
	RevertedTransaction   string = "REVERTED_TRANSACTION"
)

type ledgerMonitor struct {
	publisher message.Publisher
	topics    map[string]string
}

func (l *ledgerMonitor) publish(ctx context.Context, topic string, ledger string, et string, data interface{}) {
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

func (l *ledgerMonitor) process(ctx context.Context, ledger string, event string, data interface{}) {
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

type MonitorOption func(monitor *ledgerMonitor)

func WithLedgerMonitorGlobalTopic(v string) MonitorOption {
	return func(monitor *ledgerMonitor) {
		monitor.topics["*"] = v
	}
}

func WithLedgerMonitorTopic(key string, v string) MonitorOption {
	return func(monitor *ledgerMonitor) {
		monitor.topics[key] = v
	}
}

func WithLedgerMonitorTopics(kv map[string]string) MonitorOption {
	return func(monitor *ledgerMonitor) {
		for k, v := range kv {
			monitor.topics[k] = v
		}
	}
}

func NewLedgerMonitor(publisher message.Publisher, opts ...MonitorOption) *ledgerMonitor {
	m := &ledgerMonitor{
		publisher: publisher,
		topics:    map[string]string{},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func ProvideMonitorOption(constructor interface{}) fx.Option {
	return fx.Provide(fx.Annotate(constructor, fx.ResultTags(`group:"monitorOptions"`)))
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
