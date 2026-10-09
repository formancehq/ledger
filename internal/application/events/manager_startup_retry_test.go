package events

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

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

type startupGateTestSink struct {
	called       atomic.Int64
	allowSuccess atomic.Bool
}

func (s *startupGateTestSink) Publish(context.Context, []*eventspb.Event) error {
	s.called.Add(1)
	if !s.allowSuccess.Load() {
		return errors.New("delivery blocked")
	}

	return nil
}

func (*startupGateTestSink) Close() error { return nil }

type startupStatusProposer struct {
	store   *dal.Store
	updates atomic.Int64
}

func applyStartupStatusUpdate(store *dal.Store, update *raftcmdpb.EventsSinkUpdate) error {
	batch := store.OpenWriteSession()
	var err error
	if update.GetCursor() > 0 {
		err = state.SetSinkCursor(batch, update.GetSinkName(), update.GetCursor())
	}
	if err != nil {
		_ = batch.Cancel()

		return err
	}
	if update.GetClearError() {
		err = state.ClearSinkStatus(batch, update.GetSinkName())
	} else if update.GetError() != nil {
		err = state.SetSinkStatus(batch, &commonpb.SinkStatus{SinkName: update.GetSinkName(), Error: update.GetError()})
	}
	if err != nil {
		_ = batch.Cancel()

		return err
	}

	return batch.Commit()
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
		err := applyStartupStatusUpdate(p.store, update)
		if err != nil {
			f.Resolve(state.ApplyResult{}, err)

			return f, nil
		}
		if update.GetError() != nil || update.GetClearError() || update.GetCursor() > 0 {
			p.updates.Add(1)
		}
	}
	f.Resolve(state.ApplyResult{}, nil)

	return f, nil
}

// delayedStartupStatusProposer accepts an error report but applies it only
// before a later clear, as an ordered Raft log can do after the first wait times out.
type delayedStartupStatusProposer struct {
	store           *dal.Store
	mu              sync.Mutex
	barrierOnce     sync.Once
	clearOnce       sync.Once
	pending         *raftcmdpb.EventsSinkUpdate
	pendingFuture   *futures.Future[state.ApplyResult]
	barrierProposed chan struct{}
	clearProposed   chan struct{}
	release         chan struct{}
	reportApplied   chan struct{}
	releaseClear    chan struct{}
}

func (p *delayedStartupStatusProposer) Propose(_ context.Context, proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	cmd := &raftcmdpb.Proposal{}
	if err := cmd.UnmarshalVT(proposal.Data()); err != nil {
		return nil, err
	}
	update := cmd.GetTechnicalUpdates()[0].GetEventsSink()
	proposal.Resolve(nil, nil)
	f := futures.New[state.ApplyResult]()
	if update.GetError() != nil && strings.HasPrefix(update.GetError().GetMessage(), sinkStartupErrorPrefix) {
		p.mu.Lock()
		p.pending = update
		p.pendingFuture = f
		p.mu.Unlock()

		return f, nil
	}
	if update.GetError() != nil {
		f.Resolve(state.ApplyResult{}, applyStartupStatusUpdate(p.store, update))

		return f, nil
	}
	if update.GetClearError() {
		p.clearOnce.Do(func() { close(p.clearProposed) })
		go func() {
			<-p.releaseClear
			f.Resolve(state.ApplyResult{}, applyStartupStatusUpdate(p.store, update))
		}()

		return f, nil
	}
	p.mu.Lock()
	hasPending := p.pending != nil
	p.mu.Unlock()
	if !hasPending {
		f.Resolve(state.ApplyResult{}, applyStartupStatusUpdate(p.store, update))

		return f, nil
	}
	p.barrierOnce.Do(func() { close(p.barrierProposed) })
	go func() {
		<-p.release
		p.mu.Lock()
		pending, pendingFuture := p.pending, p.pendingFuture
		p.pending, p.pendingFuture = nil, nil
		p.mu.Unlock()
		if pending != nil {
			err := applyStartupStatusUpdate(p.store, pending)
			pendingFuture.Resolve(state.ApplyResult{}, err)
			if err != nil {
				f.Resolve(state.ApplyResult{}, err)

				return
			}
			close(p.reportApplied)
		}
		f.Resolve(state.ApplyResult{}, applyStartupStatusUpdate(p.store, update))
	}()

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

func TestManager_UncertainStartupReportClearedAfterRecovery(t *testing.T) {
	previous, existed := sinkFactories["nats"]
	t.Cleanup(func() {
		if existed {
			sinkFactories["nats"] = previous
		} else {
			delete(sinkFactories, "nats")
		}
	})
	var attempts atomic.Int64
	sink := &startupGateTestSink{}
	sinkFactories["nats"] = func(*commonpb.SinkConfig, Format) (Sink, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("temporary startup failure")
		}

		return sink, nil
	}
	builder, store := newTestBuilder(t)
	attrs := attributes.New()
	config := &commonpb.SinkConfig{Name: "uncertain-startup-report", Type: &commonpb.SinkConfig_Nats{
		Nats: &commonpb.NatsSinkConfig{Url: "nats://dependency.invalid:4222", Topic: "ledger.events"},
	}}
	saveManagedSinkConfig(t, attrs, store, config)
	batch := store.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: "orders", CreatedAt: commonpb.NewTimestamp(libtime.Now())},
		}},
	}}))
	require.NoError(t, state.SetAppliedIndex(batch, 1))
	require.NoError(t, batch.Commit())
	proposer := &delayedStartupStatusProposer{store: store, barrierProposed: make(chan struct{}), clearProposed: make(chan struct{}), release: make(chan struct{}), reportApplied: make(chan struct{}), releaseClear: make(chan struct{})}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(proposer.release) }) })
	var clearOnce sync.Once
	t.Cleanup(func() { clearOnce.Do(func() { close(proposer.releaseClear) }) })
	m := NewManager(store, attrs, proposer, builder, logging.Testing(), signal.NewNotifications())
	m.Start()
	t.Cleanup(m.Stop)
	m.OnLeadershipChange(true)
	require.Eventually(t, func() bool { return attempts.Load() >= 2 }, 5*time.Second, 10*time.Millisecond)
	select {
	case <-proposer.barrierProposed:
	case <-time.After(2 * time.Second):
		t.Fatal("a successful start must wait for accepted status updates")
	}
	releaseOnce.Do(func() { close(proposer.release) })
	select {
	case <-proposer.reportApplied:
	case <-time.After(2 * time.Second):
		t.Fatal("the accepted startup report did not apply")
	}
	status, err := startupStatusView(store, config)
	require.NoError(t, err)
	require.Equal(t, "sink startup: temporary startup failure", status.GetError().GetMessage())
	select {
	case <-proposer.clearProposed:
	case <-time.After(2 * time.Second):
		t.Fatal("the startup error was not cleared after the status barrier")
	}
	require.Never(t, func() bool { return sink.called.Load() > 0 },
		300*time.Millisecond, 10*time.Millisecond,
		"the emitter must not publish while its startup clear is pending")
	clearOnce.Do(func() { close(proposer.releaseClear) })
	require.Eventually(t, func() bool {
		status, err := startupStatusView(store, config)

		return err == nil && sink.called.Load() > 0 &&
			status.GetError().GetMessage() == "delivery blocked" &&
			managedSinkByName(m, config.GetName()) != nil
	}, 3*time.Second, 10*time.Millisecond, "a delivery error after startup must survive the startup clear")
	sink.allowSuccess.Store(true)
	managedSinkByName(m, config.GetName()).emitter.Notify()
	require.Eventually(t, func() bool {
		status, err := startupStatusView(store, config)

		return err == nil && status.GetError() == nil && status.GetCursor() == 1
	}, 4*time.Second, 10*time.Millisecond)
}

func TestManager_PreservesDeliveryErrorAcrossStartupFailure(t *testing.T) {
	previous, existed := sinkFactories["nats"]
	t.Cleanup(func() {
		if existed {
			sinkFactories["nats"] = previous
		} else {
			delete(sinkFactories, "nats")
		}
	})
	allowSuccess := make(chan struct{})
	sinkFactories["nats"] = func(*commonpb.SinkConfig, Format) (Sink, error) {
		select {
		case <-allowSuccess:
			return startupRetryTestSink{}, nil
		default:
			return nil, errors.New("constructor unavailable")
		}
	}
	builder, store := newTestBuilder(t)
	attrs := attributes.New()
	config := &commonpb.SinkConfig{Name: "prior-delivery-error", Type: &commonpb.SinkConfig_Nats{
		Nats: &commonpb.NatsSinkConfig{Url: "nats://dependency.invalid:4222", Topic: "ledger.events"},
	}}
	saveManagedSinkConfig(t, attrs, store, config)
	batch := store.OpenWriteSession()
	require.NoError(t, state.SetSinkStatus(batch, &commonpb.SinkStatus{SinkName: config.GetName(), Error: &commonpb.SinkError{Message: "delivery failed"}}))
	require.NoError(t, batch.Commit())
	proposer := &startupStatusProposer{store: store}
	m := NewManager(store, attrs, proposer, builder, logging.Testing(), signal.NewNotifications())
	m.Start()
	t.Cleanup(m.Stop)
	m.OnLeadershipChange(true)
	require.Eventually(t, func() bool { return pendingStartupRetry(m, config.GetName()) }, time.Second, 10*time.Millisecond)
	status, err := startupStatusView(store, config)
	require.NoError(t, err)
	require.Equal(t, "delivery failed", status.GetError().GetMessage())
	require.Zero(t, proposer.updates.Load())
	close(allowSuccess)
	require.Eventually(t, func() bool { return managedSinkByName(m, config.GetName()) != nil }, 3*time.Second, 10*time.Millisecond)
	status, err = startupStatusView(store, config)
	require.NoError(t, err)
	require.Equal(t, "delivery failed", status.GetError().GetMessage())
	require.Zero(t, proposer.updates.Load())

	batch = store.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{
				Name: "orders", CreatedAt: commonpb.NewTimestamp(libtime.Now()),
			},
		}},
	}}))
	require.NoError(t, state.SetAppliedIndex(batch, 1))
	require.NoError(t, batch.Commit())
	managedSinkByName(m, config.GetName()).emitter.Notify()
	require.Eventually(t, func() bool {
		status, err := startupStatusView(store, config)

		return err == nil && status.GetError() == nil && status.GetCursor() == 1
	}, 3*time.Second, 10*time.Millisecond, "the delivery error clears only after a successful publish")
}

func TestManager_PreservesPendingDeliveryErrorAcrossStartup(t *testing.T) {
	previous, existed := sinkFactories["nats"]
	t.Cleanup(func() {
		if existed {
			sinkFactories["nats"] = previous
		} else {
			delete(sinkFactories, "nats")
		}
	})
	sinkFactories["nats"] = func(*commonpb.SinkConfig, Format) (Sink, error) {
		return startupRetryTestSink{}, nil
	}
	builder, store := newTestBuilder(t)
	attrs := attributes.New()
	config := &commonpb.SinkConfig{Name: "pending-delivery-error", Type: &commonpb.SinkConfig_Nats{
		Nats: &commonpb.NatsSinkConfig{Url: "nats://dependency.invalid:4222", Topic: "ledger.events"},
	}}
	saveManagedSinkConfig(t, attrs, store, config)
	proposer := &delayedStartupStatusProposer{
		store: store, barrierProposed: make(chan struct{}), clearProposed: make(chan struct{}),
		release: make(chan struct{}), reportApplied: make(chan struct{}), releaseClear: make(chan struct{}),
		pending:       &raftcmdpb.EventsSinkUpdate{SinkName: config.GetName(), Error: &commonpb.SinkError{Message: "delivery failed"}},
		pendingFuture: futures.New[state.ApplyResult](),
	}
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(proposer.release) }) })
	m := NewManager(store, attrs, proposer, builder, logging.Testing(), signal.NewNotifications())
	m.Start()
	t.Cleanup(m.Stop)
	m.OnLeadershipChange(true)
	select {
	case <-proposer.barrierProposed:
	case <-time.After(2 * time.Second):
		t.Fatal("startup did not wait for the pending delivery update")
	}
	status, err := startupStatusView(store, config)
	require.NoError(t, err)
	require.Nil(t, status.GetError(), "the delivery update is still pending")
	releaseOnce.Do(func() { close(proposer.release) })
	require.Eventually(t, func() bool {
		status, err := startupStatusView(store, config)

		return err == nil && status.GetError().GetMessage() == "delivery failed" &&
			managedSinkByName(m, config.GetName()) != nil
	}, 3*time.Second, 10*time.Millisecond)
	select {
	case <-proposer.clearProposed:
		t.Fatal("startup cleared a previously accepted delivery error")
	default:
	}
}
