package events

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type constructorLifecycleSink struct{ closes atomic.Int64 }

func (*constructorLifecycleSink) Publish(context.Context, []*eventspb.Event) error { return nil }
func (s *constructorLifecycleSink) Close() error {
	s.closes.Add(1)

	return nil
}

func replaceConstructorFactory(t *testing.T, factory sinkFactory) {
	t.Helper()
	// Registry mutation tests must remain sequential, as in the startup retry tests.
	previous, existed := sinkFactories["nats"]
	sinkFactories["nats"] = factory
	t.Cleanup(func() {
		if existed {
			sinkFactories["nats"] = previous
		} else {
			delete(sinkFactories, "nats")
		}
	})
}

func waitConstructorSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal(message)
	}
}

func TestManager_ClosesStaleConstructorBeforeEmitterStartup(t *testing.T) {
	for _, stop := range []bool{false, true} {
		name := "loss-regain"
		if stop {
			name = "stop"
		}
		t.Run(name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			stale, successor := &constructorLifecycleSink{}, &constructorLifecycleSink{}
			var attempts, cursorReads atomic.Int64
			replaceConstructorFactory(t, func(context.Context, *commonpb.SinkConfig, Format) (Sink, error) {
				if attempts.Add(1) == 1 {
					close(entered)
					<-release

					return stale, nil
				}

				return successor, nil
			})
			builder, store := newTestBuilder(t)
			attrs := attributes.New()
			config := &commonpb.SinkConfig{Name: "blocked-constructor", Type: &commonpb.SinkConfig_Nats{Nats: &commonpb.NatsSinkConfig{}}}
			saveManagedSinkConfig(t, attrs, store, config)
			proposer := &startupStatusProposer{store: store}
			m := NewManager(store, attrs, proposer, builder, logging.Testing(), signal.NewNotifications())
			m.readSinkCursor = func(dal.PebbleGetter, string) (uint64, error) {
				cursorReads.Add(1)

				return 0, nil
			}
			m.Start()
			// Release construction before joining even when a regression assertion fails.
			var stopDone <-chan struct{}
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				m.Stop()
				if stopDone != nil {
					waitConstructorSignal(t, stopDone, "Stop did not finish after cleanup released construction")
				}
			})
			m.OnLeadershipChange(true)
			waitConstructorSignal(t, entered, "constructor did not enter")
			stopped := make(chan struct{})
			if stop {
				stopDone = stopped
				go func() { m.Stop(); close(stopped) }()
				require.Eventually(t, func() bool {
					_, _, terminal := m.leadershipSnapshot()

					return terminal
				}, time.Second, time.Millisecond)
			} else {
				m.OnLeadershipChange(false)
				m.OnLeadershipChange(true)
			}
			releaseOnce.Do(func() { close(release) })
			if stop {
				waitConstructorSignal(t, stopped, "Stop did not join released construction")
			} else {
				require.Eventually(t, func() bool { return managedSinkByName(m, config.GetName()) != nil }, 3*time.Second, time.Millisecond)
				m.Stop()
			}
			require.Equal(t, int64(1), stale.closes.Load(), "stale success remains manager-owned until closed")
			expectedReads, expectedAttempts, expectedSuccessorCloses := int64(1), int64(2), int64(1)
			if stop {
				expectedReads, expectedAttempts, expectedSuccessorCloses = 0, 1, 0
			}
			require.Equal(t, expectedReads, cursorReads.Load(), "obsolete construction must not start an emitter")
			require.Equal(t, expectedAttempts, attempts.Load())
			require.Equal(t, expectedSuccessorCloses, successor.closes.Load())
			require.Zero(t, proposer.updates.Load(), "obsolete construction must not report a startup error")
			require.Empty(t, m.emitters)
			require.Empty(t, m.retries)
		})
	}
}

func TestManager_CancelsInProgressSinkConstruction(t *testing.T) {
	for _, stop := range []bool{false, true} {
		name := "loss-regain"
		if stop {
			name = "stop"
		}
		t.Run(name, func(t *testing.T) {
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			var attempts atomic.Int64
			successor := &constructorLifecycleSink{}
			replaceConstructorFactory(t, func(ctx context.Context, _ *commonpb.SinkConfig, _ Format) (Sink, error) {
				if attempts.Add(1) > 1 {
					return successor, nil
				}
				close(entered)
				select {
				case <-ctx.Done():
					close(canceled)

					return nil, ctx.Err()
				case <-release:
					return nil, context.Canceled
				}
			})
			builder, store := newTestBuilder(t)
			attrs := attributes.New()
			config := &commonpb.SinkConfig{Name: "cancel-constructor", Type: &commonpb.SinkConfig_Nats{Nats: &commonpb.NatsSinkConfig{}}}
			saveManagedSinkConfig(t, attrs, store, config)
			proposer := &startupStatusProposer{store: store}
			m := NewManager(store, attrs, proposer, builder, logging.Testing(), signal.NewNotifications())
			m.Start()
			var stopDone <-chan struct{}
			t.Cleanup(func() {
				releaseOnce.Do(func() { close(release) })
				m.Stop()
				if stopDone != nil {
					waitConstructorSignal(t, stopDone, "Stop did not finish after cleanup released construction")
				}
			})
			m.OnLeadershipChange(true)
			waitConstructorSignal(t, entered, "constructor did not enter")
			stopped := make(chan struct{})
			if stop {
				stopDone = stopped
				go func() { m.Stop(); close(stopped) }()
			} else {
				m.OnLeadershipChange(false)
				m.OnLeadershipChange(true)
			}
			waitConstructorSignal(t, canceled, "generation cancellation did not reach active construction")
			if stop {
				waitConstructorSignal(t, stopped, "Stop did not join canceled construction")
			} else {
				require.Eventually(t, func() bool { return managedSinkByName(m, config.GetName()) != nil }, 3*time.Second, time.Millisecond,
					"the successor must reconcile after the old constructor observes cancellation")
				m.Stop()
			}
			expectedAttempts, expectedCloses := int64(2), int64(1)
			if stop {
				expectedAttempts, expectedCloses = 1, 0
			}
			require.Equal(t, expectedAttempts, attempts.Load(), "canceled construction must not create a retry")
			require.Equal(t, expectedCloses, successor.closes.Load())
			require.Zero(t, proposer.updates.Load(), "obsolete cancellation must not update startup status")
			require.Empty(t, m.emitters)
			require.Empty(t, m.retries)
		})
	}
}
