package events_test

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/go-libs/v3/logging"
	libtime "github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/ledger-v3-poc/internal/application/events"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/futures"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// recordingSink captures published events for test assertions.
type recordingSink struct {
	mu     sync.Mutex
	events []*eventspb.Event
}

func (s *recordingSink) Publish(_ context.Context, evts []*eventspb.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.events = append(s.events, evts...)

	return nil
}

func (s *recordingSink) Close() error { return nil }

func (s *recordingSink) getEvents() []*eventspb.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]*eventspb.Event, len(s.events))
	copy(out, s.events)

	return out
}

// directProposer simulates the FSM by directly applying per-sink cursor/status updates to PebbleDB.
// In production, the emitter proposes through Raft and the FSM applies the updates;
// in tests, we short-circuit by writing directly via a batch.
type directProposer struct {
	store *dal.Store
}

func (p *directProposer) Propose(proposal *node.Proposal) (*futures.Future[state.ApplyResult], error) {
	cmd := &raftcmdpb.Proposal{}

	err := cmd.UnmarshalVT(proposal.Data())
	if err != nil {
		f := futures.New[state.ApplyResult]()
		f.Resolve(state.ApplyResult{}, err)

		return f, nil
	}

	// Simulate FSM: apply per-sink updates
	for _, update := range cmd.GetEventsSinkUpdates() {
		batch := p.store.NewBatch()
		if update.GetCursor() > 0 {
			err := state.SetSinkCursor(batch, update.GetSinkName(), update.GetCursor())
			if err != nil {
				_ = batch.Cancel()
				f := futures.New[state.ApplyResult]()
				f.Resolve(state.ApplyResult{}, err)

				return f, nil
			}
		}

		if update.GetClearError() {
			err := state.ClearSinkStatus(batch, update.GetSinkName())
			if err != nil {
				_ = batch.Cancel()
				f := futures.New[state.ApplyResult]()
				f.Resolve(state.ApplyResult{}, err)

				return f, nil
			}
		} else if update.GetError() != nil {
			err := state.SetSinkStatus(batch, &commonpb.SinkStatus{
				SinkName: update.GetSinkName(),
				Cursor:   update.GetCursor(),
				Error:    update.GetError(),
			})
			if err != nil {
				_ = batch.Cancel()
				f := futures.New[state.ApplyResult]()
				f.Resolve(state.ApplyResult{}, err)

				return f, nil
			}
		}

		err := batch.Commit()
		if err != nil {
			f := futures.New[state.ApplyResult]()
			f.Resolve(state.ApplyResult{}, err)

			return f, nil
		}
	}

	f := futures.New[state.ApplyResult]()
	f.Resolve(state.ApplyResult{}, nil)

	return f, nil
}

func newTestStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func appendTestLogs(t *testing.T, s *dal.Store, logs ...*commonpb.Log) {
	t.Helper()

	batch := s.NewBatch()
	require.NoError(t, state.AppendLogs(batch, logs...))
	require.NoError(t, state.SetAppliedIndex(batch, 1))
	require.NoError(t, batch.Commit())
}

func registerLedger(t *testing.T, s *dal.Store, name string) {
	t.Helper()

	batch := s.NewBatch()
	require.NoError(t, state.SaveLedger(batch, &commonpb.LedgerInfo{
		Name:      name,
		CreatedAt: commonpb.NewTimestamp(libtime.Now()),
	}))
	require.NoError(t, batch.Commit())
}

func TestEmitterIntegration_ProcessExistingLogs(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	sink := &recordingSink{}
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	// Write logs before starting emitter (simulates catch-up on leader restart)
	registerLedger(t, store, "orders")

	now := libtime.Now()

	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreateLedgerLog{
						Info: &commonpb.LedgerInfo{
							Name:      "orders",
							CreatedAt: commonpb.NewTimestamp(now),
						},
					},
				},
			},
		},
		&commonpb.Log{
			Sequence: 2,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(
											commonpb.NewPosting("world", "bank", "USD", big.NewInt(1000)),
										).
										WithID(1).
										WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
	)

	// Start emitter — it should catch up on existing logs
	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "test-sink", proposer, logger, cfg)
	emitter.Start()

	// Wait for the emitter to process
	require.Eventually(t, func() bool {
		return len(sink.getEvents()) >= 2
	}, 5*time.Second, 10*time.Millisecond, "emitter should process existing logs")

	emitter.Stop()

	published := sink.getEvents()
	require.Len(t, published, 2)
	require.Equal(t, commonpb.EventType_CREATED_LEDGER, published[0].GetType())
	require.Equal(t, "orders", published[0].GetLedger())
	require.Equal(t, uint64(1), published[0].GetLogSequence())
	require.Equal(t, commonpb.EventType_COMMITTED_TRANSACTION, published[1].GetType())
	require.Equal(t, "orders", published[1].GetLedger())
	require.Equal(t, uint64(2), published[1].GetLogSequence())

	// Verify cursor was advanced
	cursor, err := query.ReadSinkCursor(store, "test-sink")
	require.NoError(t, err)
	require.Equal(t, uint64(2), cursor)
}

func TestEmitterIntegration_NotificationDrivenProcessing(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	sink := &recordingSink{}
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "payments")

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	cfg.BatchDelay = 1 * time.Second // long delay so we test notification-driven path
	emitter := events.NewEmitter(store, sink, "test-sink", proposer, logger, cfg)

	emitter.Start()
	defer emitter.Stop()

	// Verify no events emitted when there are no logs
	require.Never(t, func() bool {
		return len(sink.getEvents()) > 0
	}, 100*time.Millisecond, 10*time.Millisecond, "emitter should not emit events when there are no logs")

	// Append logs and notify
	now := libtime.Now()
	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "payments",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(
											commonpb.NewPosting("world", "merchant", "EUR", big.NewInt(500)),
										).
										WithID(1).
										WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
	)

	// Trigger notification
	emitter.Notify()

	require.Eventually(t, func() bool {
		return len(sink.getEvents()) >= 1
	}, 5*time.Second, 10*time.Millisecond, "emitter should process after notification")

	published := sink.getEvents()
	require.Equal(t, commonpb.EventType_COMMITTED_TRANSACTION, published[0].GetType())
	require.Equal(t, "payments", published[0].GetLedger())
}

func TestEmitterIntegration_CursorResumesAfterRestart(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	sink := &recordingSink{}
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "orders")

	now := libtime.Now()

	// Append 3 logs
	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreateLedgerLog{
						Info: &commonpb.LedgerInfo{Name: "orders", CreatedAt: commonpb.NewTimestamp(now)},
					},
				},
			},
		},
		&commonpb.Log{
			Sequence: 2,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "bank", "USD", big.NewInt(100))).
										WithID(1).WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
		&commonpb.Log{
			Sequence: 3,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_SavedMetadata{
								SavedMetadata: &commonpb.SavedMetadata{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Account{
											Account: &commonpb.TargetAccount{Addr: "bank"},
										},
									},
									Metadata: commonpb.MetadataSetFromMap(map[string]string{"type": "asset"}),
								},
							},
						}).WithID(2).WithDate(now),
					},
				},
			},
		},
	)

	// First emitter run: processes all 3 logs
	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter1 := events.NewEmitter(store, sink, "test-sink", proposer, logger, cfg)
	emitter1.Start()

	require.Eventually(t, func() bool {
		return len(sink.getEvents()) >= 3
	}, 5*time.Second, 10*time.Millisecond, "first emitter should process all 3 logs")

	emitter1.Stop()

	// Verify cursor is at 3
	cursor, err := query.ReadSinkCursor(store, "test-sink")
	require.NoError(t, err)
	require.Equal(t, uint64(3), cursor)

	// Append more logs
	appendTestLogs(t, store,
		&commonpb.Log{
			Sequence: 4,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "orders",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("bank", "user", "USD", big.NewInt(50))).
										WithID(2).WithTimestamp(now),
								},
							},
						}).WithID(3).WithDate(now),
					},
				},
			},
		},
	)

	// Second emitter (simulates leader restart): should only process log 4
	sink2 := &recordingSink{}
	emitter2 := events.NewEmitter(store, sink2, "test-sink", proposer, logger, cfg)
	emitter2.Start()

	require.Eventually(t, func() bool {
		return len(sink2.getEvents()) >= 1
	}, 5*time.Second, 10*time.Millisecond, "second emitter should process only new logs")

	emitter2.Stop()

	published := sink2.getEvents()
	require.Len(t, published, 1)
	require.Equal(t, commonpb.EventType_COMMITTED_TRANSACTION, published[0].GetType())
	require.Equal(t, uint64(4), published[0].GetLogSequence())

	// Final cursor should be at 4
	cursor, err = query.ReadSinkCursor(store, "test-sink")
	require.NoError(t, err)
	require.Equal(t, uint64(4), cursor)
}

func TestEmitterIntegration_AllEventTypes(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	sink := &recordingSink{}
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "test")

	now := libtime.Now()

	// Write logs covering all 6 event types
	appendTestLogs(t, store,
		// 1: CREATED_LEDGER
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreateLedgerLog{
						Info: &commonpb.LedgerInfo{Name: "test", CreatedAt: commonpb.NewTimestamp(now)},
					},
				},
			},
		},
		// 2: COMMITTED_TRANSACTION
		&commonpb.Log{
			Sequence: 2,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "test",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "bank", "USD", big.NewInt(100))).
										WithID(1).WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
		// 3: REVERTED_TRANSACTION
		&commonpb.Log{
			Sequence: 3,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "test",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
								RevertedTransaction: &commonpb.RevertedTransaction{
									RevertedTransactionId: 1,
									RevertTransaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("bank", "world", "USD", big.NewInt(100))).
										WithID(2).WithTimestamp(now),
								},
							},
						}).WithID(2).WithDate(now),
					},
				},
			},
		},
		// 4: SAVED_METADATA
		&commonpb.Log{
			Sequence: 4,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "test",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_SavedMetadata{
								SavedMetadata: &commonpb.SavedMetadata{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Account{
											Account: &commonpb.TargetAccount{Addr: "bank"},
										},
									},
									Metadata: commonpb.MetadataSetFromMap(map[string]string{"k": "v"}),
								},
							},
						}).WithID(3).WithDate(now),
					},
				},
			},
		},
		// 5: DELETED_METADATA
		&commonpb.Log{
			Sequence: 5,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "test",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
								DeletedMetadata: &commonpb.DeletedMetadata{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Account{
											Account: &commonpb.TargetAccount{Addr: "bank"},
										},
									},
									Key: "k",
								},
							},
						}).WithID(4).WithDate(now),
					},
				},
			},
		},
		// 6: DELETED_LEDGER
		&commonpb.Log{
			Sequence: 6,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_DeleteLedger{
					DeleteLedger: &commonpb.DeleteLedgerLog{
						Info: &commonpb.LedgerInfo{Name: "test", DeletedAt: commonpb.NewTimestamp(now)},
					},
				},
			},
		},
	)

	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	emitter := events.NewEmitter(store, sink, "test-sink", proposer, logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		return len(sink.getEvents()) >= 6
	}, 5*time.Second, 10*time.Millisecond, "emitter should process all 6 event types")

	emitter.Stop()

	published := sink.getEvents()
	require.Len(t, published, 6)

	expectedTypes := []commonpb.EventType{
		commonpb.EventType_CREATED_LEDGER,
		commonpb.EventType_COMMITTED_TRANSACTION,
		commonpb.EventType_REVERTED_TRANSACTION,
		commonpb.EventType_SAVED_METADATA,
		commonpb.EventType_DELETED_METADATA,
		commonpb.EventType_DELETED_LEDGER,
	}

	for i, expected := range expectedTypes {
		require.Equal(t, expected, published[i].GetType(), "event %d should be %s", i, expected)
		require.Equal(t, uint64(i+1), published[i].GetLogSequence())
		require.NotNil(t, published[i].GetLog(), "event %d should carry the full Log", i)
	}
}

func TestEmitterIntegration_Batching(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	sink := &recordingSink{}
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "test")

	now := libtime.Now()

	// Write 10 logs
	var logs []*commonpb.Log
	for i := uint64(1); i <= 10; i++ {
		logs = append(logs, &commonpb.Log{
			Sequence: i,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "test",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "bank", "USD", big.NewInt(int64(i*100)))).
										WithID(i).WithTimestamp(now),
								},
							},
						}).WithID(i).WithDate(now),
					},
				},
			},
		})
	}

	appendTestLogs(t, store, logs...)

	// Use small batch size to verify batching works
	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 3
	emitter := events.NewEmitter(store, sink, "test-sink", proposer, logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		return len(sink.getEvents()) >= 10
	}, 5*time.Second, 10*time.Millisecond, "emitter should process all 10 logs across multiple batches")

	emitter.Stop()

	published := sink.getEvents()
	require.Len(t, published, 10)

	// Verify ordering is preserved
	for i, evt := range published {
		require.Equal(t, uint64(i+1), evt.GetLogSequence())
	}

	// Cursor should be at 10
	cursor, err := query.ReadSinkCursor(store, "test-sink")
	require.NoError(t, err)
	require.Equal(t, uint64(10), cursor)
}

func TestEmitterIntegration_EventTypeFilter(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	sink := &recordingSink{}
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	registerLedger(t, store, "test")

	now := libtime.Now()

	// Write logs covering 3 event types
	appendTestLogs(t, store,
		// 1: CREATED_LEDGER
		&commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreateLedgerLog{
						Info: &commonpb.LedgerInfo{Name: "test", CreatedAt: commonpb.NewTimestamp(now)},
					},
				},
			},
		},
		// 2: COMMITTED_TRANSACTION
		&commonpb.Log{
			Sequence: 2,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "test",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
								CreatedTransaction: &commonpb.CreatedTransaction{
									Transaction: commonpb.NewTransaction().
										WithPostings(commonpb.NewPosting("world", "bank", "USD", big.NewInt(100))).
										WithID(1).WithTimestamp(now),
								},
							},
						}).WithID(1).WithDate(now),
					},
				},
			},
		},
		// 3: SAVED_METADATA
		&commonpb.Log{
			Sequence: 3,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "test",
						Log: commonpb.NewLedgerLog(&commonpb.LedgerLogPayload{
							Payload: &commonpb.LedgerLogPayload_SavedMetadata{
								SavedMetadata: &commonpb.SavedMetadata{
									Target: &commonpb.Target{
										Target: &commonpb.Target_Account{
											Account: &commonpb.TargetAccount{Addr: "bank"},
										},
									},
									Metadata: commonpb.MetadataSetFromMap(map[string]string{"k": "v"}),
								},
							},
						}).WithID(2).WithDate(now),
					},
				},
			},
		},
	)

	// Configure emitter to only accept COMMITTED_TRANSACTION events
	cfg := events.DefaultEmitterConfig()
	cfg.BatchSize = 10
	cfg.EventTypes = map[commonpb.EventType]struct{}{
		commonpb.EventType_COMMITTED_TRANSACTION: {},
	}
	emitter := events.NewEmitter(store, sink, "filter-sink", proposer, logger, cfg)
	emitter.Start()

	require.Eventually(t, func() bool {
		cursor, err := query.ReadSinkCursor(store, "filter-sink")

		return err == nil && cursor >= 3
	}, 5*time.Second, 10*time.Millisecond, "emitter should advance cursor past all logs")

	emitter.Stop()

	// Only COMMITTED_TRANSACTION should be published; CREATED_LEDGER and SAVED_METADATA are filtered out
	published := sink.getEvents()
	require.Len(t, published, 1)
	require.Equal(t, commonpb.EventType_COMMITTED_TRANSACTION, published[0].GetType())
	require.Equal(t, uint64(2), published[0].GetLogSequence())
}

func TestEmitterIntegration_StartStopIdempotent(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	sink := &recordingSink{}
	proposer := &directProposer{store: store}
	logger := logging.Testing()

	cfg := events.DefaultEmitterConfig()
	emitter := events.NewEmitter(store, sink, "test-sink", proposer, logger, cfg)

	// Start and stop multiple times — should not panic
	emitter.Start()
	emitter.Start() // idempotent
	emitter.Stop()
	emitter.Stop() // idempotent

	// Start again after stop
	emitter.Start()
	emitter.Stop()
}
