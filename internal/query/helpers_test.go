package query_test

import (
	"errors"
	"io"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

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

func registerLedger(t *testing.T, s *dal.Store, name string) {
	t.Helper()

	batch := s.OpenWriteSession()
	err := state.SaveLedger(batch, name, &ledgerpb.LedgerInfo{
		Name:      name,
		CreatedAt: ledgerpb.NewTimestamp(libtime.Now()),
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

func appendLogs(t *testing.T, s *dal.Store, lastAppliedIndex uint64, logs ...*ledgerpb.Log) {
	t.Helper()

	batch := s.OpenWriteSession()
	err := state.AppendLogs(batch, logs)
	require.NoError(t, err)
	require.NoError(t, state.SetAppliedIndex(batch, lastAppliedIndex))
	require.NoError(t, batch.Commit())
}

func collectLedgers(c cursor.Cursor[*ledgerpb.LedgerInfo]) ([]*ledgerpb.LedgerInfo, error) {
	defer func() { _ = c.Close() }()

	var ledgers []*ledgerpb.LedgerInfo

	for {
		ledger, err := c.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, err
		}

		ledgers = append(ledgers, ledger)
	}

	return ledgers, nil
}

func collectLogs(t *testing.T, c cursor.Cursor[*ledgerpb.Log]) []*ledgerpb.Log {
	t.Helper()

	defer func() { _ = c.Close() }()

	var logs []*ledgerpb.Log

	for {
		log, err := c.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		require.NoError(t, err)

		logs = append(logs, log)
	}

	return logs
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
