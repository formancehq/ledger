package state

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// noopConfChangeHandler satisfies the confChangeHandler contract for
// tests that don't wire a real Node. The FSM's PrepareEntries calls
// this for every EntryConfChange* — a plain no-op is safe because
// tests that don't exercise membership don't propose ConfChange
// entries either.
func noopConfChangeHandler(*raftpb.Entry, *dal.WriteSession) error { return nil }

func newNoopNotifier(t *testing.T) *MockNotifier {
	t.Helper()

	n := NewMockNotifier(gomock.NewController(t))
	n.EXPECT().NotifyLogsCommitted(gomock.Any()).AnyTimes()
	n.EXPECT().NotifyConfigChanged().AnyTimes()

	return n
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

// openStoreAt opens a dal.Store at dir. Unlike newTestStore it registers no
// cleanup close, so a test driving a close/reopen lifecycle manages the handle
// itself and can reopen a new Store on the same dir.
func openStoreAt(t *testing.T, dir string) *dal.Store {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(dir, logger, meter, dal.DefaultConfig())
	require.NoError(t, err)

	return s
}

func registerLedger(t *testing.T, s *dal.Store, name string) {
	t.Helper()

	batch := s.OpenWriteSession()
	err := SaveLedger(batch, name, &ledgerpb.LedgerInfo{
		Name:      name,
		CreatedAt: ledgerpb.NewTimestamp(libtime.Now()),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

func appendLogs(t *testing.T, s *dal.Store, lastAppliedIndex uint64, logs ...*ledgerpb.Log) {
	t.Helper()

	batch := s.OpenWriteSession()
	err := AppendLogs(batch, logs)
	require.NoError(t, err)
	require.NoError(t, SetAppliedIndex(batch, lastAppliedIndex))
	require.NoError(t, batch.Commit())
}

func createTestLogs(ledgerName string) []*ledgerpb.Log {
	return createTestLogsForLedger(ledgerName, 1)
}

func createTestLogsForLedger(ledgerName string, startSequence uint64) []*ledgerpb.Log {
	now := libtime.Now()

	return []*ledgerpb.Log{
		{
			Sequence: startSequence,
			Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{
				Apply: &ledgerpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: ledgerpb.NewLedgerLog(&ledgerpb.LedgerLogPayload{
						Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &ledgerpb.CreatedTransaction{
								Transaction: ledgerpb.NewTransaction().
									WithPostings(
										ledgerpb.NewPosting("world", "bank", "USD", big.NewInt(100)),
									).
									WithID(1).
									WithTimestamp(now),
								AccountMetadata: map[string]*ledgerpb.MetadataMap{
									"bank": ledgerpb.MetadataMapFromGoMap(metadata.Metadata{
										"account_type": "asset",
									}),
								},
							},
						},
					}).
						WithID(1).
						WithDate(now),
				},
			}},
		},
		{
			Sequence: startSequence + 1,
			Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{
				Apply: &ledgerpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: ledgerpb.NewLedgerLog(&ledgerpb.LedgerLogPayload{
						Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &ledgerpb.CreatedTransaction{
								Transaction: ledgerpb.NewTransaction().
									WithPostings(
										ledgerpb.NewPosting("bank", "user", "USD", big.NewInt(50)),
									).
									WithID(2).
									WithTimestamp(now),
							},
						},
					}).
						WithID(2).
						WithDate(now.Add(libtime.Second)),
				},
			}},
		},
		{
			Sequence: startSequence + 2,
			Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{
				Apply: &ledgerpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: ledgerpb.NewLedgerLog(&ledgerpb.LedgerLogPayload{
						Payload: &ledgerpb.LedgerLogPayload_SavedMetadata{
							SavedMetadata: &ledgerpb.SavedMetadata{
								Target: &ledgerpb.Target{
									Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{
										Addr: "bank",
									}},
								},
								Metadata: ledgerpb.MetadataFromGoMap(metadata.Metadata{
									"label": "Bank Account",
								}),
							},
						},
					}).
						WithID(3).
						WithDate(now.Add(2 * libtime.Second)),
				},
			}},
		},
		{
			Sequence: startSequence + 3,
			Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{
				Apply: &ledgerpb.ApplyLedgerLog{
					LedgerName: ledgerName,
					Log: ledgerpb.NewLedgerLog(&ledgerpb.LedgerLogPayload{
						Payload: &ledgerpb.LedgerLogPayload_DeletedMetadata{
							DeletedMetadata: &ledgerpb.DeletedMetadata{
								Target: &ledgerpb.Target{
									Target: &ledgerpb.Target_Account{Account: &ledgerpb.TargetAccount{
										Addr: "bank",
									}},
								},
								Key: "old_key",
							},
						},
					}).
						WithID(4).
						WithDate(now.Add(3 * libtime.Second)),
				},
			}},
		},
	}
}
