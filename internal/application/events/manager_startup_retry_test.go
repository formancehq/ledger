package events

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

type startupRetryTestSink struct{}

func (startupRetryTestSink) Publish(context.Context, []*eventspb.Event) error { return nil }
func (startupRetryTestSink) Close() error                                     { return nil }

func pendingStartupRetry(m *Manager, name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.retries[name]

	return ok
}

func TestManager_RetriesTransientSinkConstructorFailure(t *testing.T) {
	// This test temporarily replaces a package-global factory. Keep it
	// sequential so parallel package tests remain paused until cleanup restores
	// the registry.
	previous, existed := sinkFactories["nats"]
	t.Cleanup(func() {
		if existed {
			sinkFactories["nats"] = previous

			return
		}

		delete(sinkFactories, "nats")
	})

	var attempts atomic.Int64
	sinkFactories["nats"] = func(*commonpb.SinkConfig, Format) (Sink, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("dependency temporarily unavailable")
		}

		return startupRetryTestSink{}, nil
	}

	builder, store := newTestBuilder(t)
	attrs := attributes.New()
	config := &commonpb.SinkConfig{
		Name: "transient-constructor-failure",
		Type: &commonpb.SinkConfig_Nats{
			Nats: &commonpb.NatsSinkConfig{
				Url:   "nats://dependency.invalid:4222",
				Topic: "ledger.events",
			},
		},
	}
	saveManagedSinkConfig(t, attrs, store, config)

	m := NewManager(store, attrs, nil, builder, logging.Testing(), signal.NewNotifications())
	m.Start()
	t.Cleanup(m.Stop)
	m.OnLeadershipChange(true)

	require.Eventually(t, func() bool {
		return attempts.Load() == 1 && pendingStartupRetry(m, config.GetName())
	}, time.Second, 10*time.Millisecond, "the initial leadership reconcile must try the sink constructor and schedule a retry")

	require.Eventually(t, func() bool {
		return attempts.Load() == 2 &&
			managedSinkByName(m, config.GetName()) != nil &&
			!pendingStartupRetry(m, config.GetName())
	}, 3*time.Second, 10*time.Millisecond,
		"the unchanged sink config must recover after its constructor dependency becomes available")
}

func TestManager_StopCancelsSinkConstructorRetry(t *testing.T) {
	previous, existed := sinkFactories["nats"]
	t.Cleanup(func() {
		if existed {
			sinkFactories["nats"] = previous

			return
		}

		delete(sinkFactories, "nats")
	})

	var attempts atomic.Int64
	sinkFactories["nats"] = func(*commonpb.SinkConfig, Format) (Sink, error) {
		attempts.Add(1)

		return nil, errors.New("dependency unavailable")
	}

	builder, store := newTestBuilder(t)
	attrs := attributes.New()
	config := &commonpb.SinkConfig{
		Name: "constructor-failure-during-stop",
		Type: &commonpb.SinkConfig_Nats{
			Nats: &commonpb.NatsSinkConfig{
				Url:   "nats://dependency.invalid:4222",
				Topic: "ledger.events",
			},
		},
	}
	saveManagedSinkConfig(t, attrs, store, config)

	m := NewManager(store, attrs, nil, builder, logging.Testing(), signal.NewNotifications())
	m.Start()
	m.OnLeadershipChange(true)

	require.Eventually(t, func() bool {
		return attempts.Load() == 1 && pendingStartupRetry(m, config.GetName())
	}, time.Second, 10*time.Millisecond, "the constructor failure must schedule a retry before shutdown")

	m.Stop()
	require.Never(t, func() bool {
		return attempts.Load() > 1
	}, 2*sinkStartupRetryDelay, 10*time.Millisecond,
		"manager shutdown must cancel the pending constructor retry")
}
