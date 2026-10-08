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
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type startupRetryTestSink struct{}

type startupStatusProposer struct {
	store   *dal.Store
	updates atomic.Int64
}

func (p *startupStatusProposer) Propose(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	defer proposal.Resolve(nil, nil)
	f := futures.New[state.ApplyResult]()
	cmd := &raftcmdpb.Proposal{}
	if err := cmd.UnmarshalVT(proposal.Data()); err != nil {
		f.Resolve(state.ApplyResult{}, err)

		return f, nil
	}
	for _, tu := range cmd.GetTechnicalUpdates() {
		update := tu.GetEventsSink()
		if update == nil {
			continue
		}
		batch := p.store.OpenWriteSession()
		var err error
		if update.GetClearError() {
			err = state.ClearSinkStatus(batch, update.GetSinkName())
		} else if update.GetError() != nil {
			err = state.SetSinkStatus(batch, &commonpb.SinkStatus{SinkName: update.GetSinkName(), Error: update.GetError()})
		}
		if err == nil {
			err = batch.Commit()
		} else {
			_ = batch.Cancel()
		}
		if err != nil {
			f.Resolve(state.ApplyResult{}, err)

			return f, nil
		}
		p.updates.Add(1)
	}
	f.Resolve(state.ApplyResult{}, nil)

	return f, nil
}

func startupStatusView(store *dal.Store, config *commonpb.SinkConfig) (*commonpb.SinkStatus, error) {
	handle, err := store.NewDirectReadHandle()
	if err != nil {
		return nil, err
	}
	defer func() { _ = handle.Close() }()
	statuses, err := query.BuildSinkStatuses(handle, []*commonpb.SinkConfig{config})
	if err != nil {
		return nil, err
	}

	return statuses[0], nil
}

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
	allowSuccess := make(chan struct{})
	sinkFactories["nats"] = func(*commonpb.SinkConfig, Format) (Sink, error) {
		attempts.Add(1)
		select {
		case <-allowSuccess:
		default:
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

	proposer := &startupStatusProposer{store: store}
	m := NewManager(store, attrs, proposer, builder, logging.Testing(), signal.NewNotifications())
	m.Start()
	t.Cleanup(m.Stop)
	m.OnLeadershipChange(true)

	require.Eventually(t, func() bool {
		status, err := startupStatusView(store, config)

		return err == nil && status.GetError().GetMessage() == "sink startup: dependency temporarily unavailable" &&
			status.GetError().GetOccurredAt() != nil && pendingStartupRetry(m, config.GetName())
	}, time.Second, 10*time.Millisecond, "the initial leadership reconcile must try the sink constructor and schedule a retry")
	require.Equal(t, int64(1), proposer.updates.Load())
	require.Eventually(t, func() bool { return attempts.Load() >= 2 }, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(1), proposer.updates.Load(), "unchanged constructor errors must not be proposed on every retry")
	close(allowSuccess)

	require.Eventually(t, func() bool {
		status, err := startupStatusView(store, config)

		return err == nil && status.GetError() == nil &&
			managedSinkByName(m, config.GetName()) != nil && !pendingStartupRetry(m, config.GetName())
	}, 3*time.Second, 10*time.Millisecond,
		"the unchanged sink config must recover after its constructor dependency becomes available")
	require.Equal(t, int64(2), proposer.updates.Load(), "an unchanged retry must not re-propose the same error")
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

	m := NewManager(store, attrs, &startupStatusProposer{store: store}, builder, logging.Testing(), signal.NewNotifications())
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

func TestManager_WaitStartedFailureIsReportedAndCleared(t *testing.T) {
	t.Parallel()
	builder, store := newTestBuilder(t)
	attrs := attributes.New()
	config := &commonpb.SinkConfig{Name: "cursor-read-failure", Type: &commonpb.SinkConfig_Http{
		Http: &commonpb.HttpSinkConfig{Endpoint: "https://example.invalid/events"},
	}}
	saveManagedSinkConfig(t, attrs, store, config)
	proposer := &startupStatusProposer{store: store}
	m := NewManager(store, attrs, proposer, builder, logging.Testing(), signal.NewNotifications())
	var failRead atomic.Bool
	var reads atomic.Int64
	failRead.Store(true)
	m.readSinkCursor = func(reader dal.PebbleGetter, name string) (uint64, error) {
		reads.Add(1)
		if failRead.Load() {
			return 0, errors.New("cursor temporarily unavailable")
		}

		return query.ReadSinkCursor(reader, name)
	}
	m.Start()
	t.Cleanup(m.Stop)
	m.OnLeadershipChange(true)
	require.Eventually(t, func() bool {
		status, err := startupStatusView(store, config)

		return err == nil && status.GetError() != nil &&
			status.GetError().GetOccurredAt() != nil && pendingStartupRetry(m, config.GetName())
	}, time.Second, 10*time.Millisecond)
	require.Contains(t, func() string {
		status, _ := startupStatusView(store, config)

		return status.GetError().GetMessage()
	}(), "sink startup:")
	require.Eventually(t, func() bool { return reads.Load() >= 2 }, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(1), proposer.updates.Load(), "unchanged startup read errors must not be proposed on every retry")

	failRead.Store(false)
	require.Eventually(t, func() bool {
		status, err := startupStatusView(store, config)

		return err == nil && status.GetError() == nil && managedSinkByName(m, config.GetName()) != nil
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, int64(2), proposer.updates.Load())
}
