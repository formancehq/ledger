package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func testLogger() logging.Logger {
	return logging.FromContext(logging.TestingContext())
}

func newRebuildTestStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

func coldLogKey(seq uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdLog).
		PutUint64(seq).
		Build()
}

func coldAuditKey(seq uint64) []byte {
	return dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).
		PutUint64(seq).
		Build()
}

// createLedgerLog builds a log whose replay writes a ledger row to the global
// zone, so the test can observe whether the rebuild batch was committed.
func createLedgerLog(seq uint64, name string, id uint32) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{
				CreateLedger: &commonpb.CreatedLedgerLog{Name: name, Id: id},
			},
		},
	}
}

func applyLedgerLog(seq uint64, ledger string, payload *commonpb.LedgerLogPayload) *commonpb.Log {
	return &commonpb.Log{
		Sequence: seq,
		Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_Apply{
				Apply: &commonpb.ApplyLedgerLog{
					LedgerName: ledger,
					Log: &commonpb.LedgerLog{
						Id:   seq,
						Data: payload,
					},
				},
			},
		},
	}
}

func addAccountTypePayload(name, pattern string, persistence commonpb.AccountTypePersistence) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_AddedAccountType{
			AddedAccountType: &commonpb.AddedAccountTypeLog{
				AccountType: &commonpb.AccountType{
					Name:        name,
					Pattern:     pattern,
					Persistence: persistence,
				},
			},
		},
	}
}

func createdTransactionPayload(id uint64, postings ...*commonpb.Posting) *commonpb.LedgerLogPayload {
	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{
					Id:       id,
					Postings: postings,
				},
			},
		},
	}
}

func rebuildTestPosting(source, destination, asset string, amount uint64) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: destination,
		Asset:       asset,
		Amount:      commonpb.NewUint256FromUint64(amount),
	}
}

func auditSuccess(seq, minLogSeq, maxLogSeq uint64) *auditpb.AuditEntry {
	return &auditpb.AuditEntry{
		Sequence: seq,
		Outcome: &auditpb.AuditEntry_Success{
			Success: &auditpb.AuditSuccess{
				MinLogSequence: minLogSeq,
				MaxLogSequence: maxLogSeq,
			},
		},
	}
}

func TestRebuildDelta_CleanEOFSucceeds(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.NewBatch()
	for seq := uint64(1); seq <= 3; seq++ {
		require.NoError(t, batch.SetProto(coldLogKey(seq), createLedgerLog(seq, "ledger", uint32(seq))))
	}
	require.NoError(t, batch.Commit())

	// A clean stream must terminate via io.EOF and report success.
	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	// Derived state was committed.
	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	info, err := query.GetLedgerByName(context.Background(), handle, "ledger")
	require.NoError(t, err)
	require.NotNil(t, info, "ledger should have been rebuilt on a clean stream")
}

func TestRebuildDelta_TruncatedStreamReturnsErrorAndDoesNotCommit(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	// Valid log at seq 1 (creates "before" ledger), then a corrupt record at
	// seq 2 whose bytes fail to unmarshal — simulating a truncated/corrupted
	// log stream during a restore.
	batch := store.NewBatch()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "before-corruption", 1)))
	require.NoError(t, batch.SetBytes(coldLogKey(2), []byte{0xff, 0xff, 0xff, 0xff}))
	require.NoError(t, batch.Commit())

	err := RebuildDelta(context.Background(), testLogger(), store, 0, 0)

	// The non-EOF cursor error must surface, not be swallowed as success.
	require.Error(t, err, "RebuildDelta must not report success on a truncated stream")

	// And the partial batch (the seq-1 ledger processed before the corrupt
	// record) must have been cancelled, not committed.
	handle, err2 := store.NewDirectReadHandle()
	require.NoError(t, err2)
	defer func() { _ = handle.Close() }()

	_, err2 = query.GetLedgerByName(context.Background(), handle, "before-corruption")
	require.ErrorIs(t, err2, domain.ErrNotFound,
		"partial rebuild state must not be committed when the stream errors")
}

func TestRebuildDelta_ReplaysEphemeralPurgeAtProposalBoundary(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.NewBatch()
	require.NoError(t, batch.SetProto(coldLogKey(1), createLedgerLog(1, "ledger", 1)))
	require.NoError(t, batch.SetProto(coldLogKey(2), applyLedgerLog(2, "ledger",
		addAccountTypePayload("orders", "orders:{id}", commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL),
	)))
	require.NoError(t, batch.SetProto(coldLogKey(3), applyLedgerLog(3, "ledger",
		createdTransactionPayload(1, rebuildTestPosting("world", "orders:1", "USD", 5)),
	)))
	require.NoError(t, batch.SetProto(coldLogKey(4), applyLedgerLog(4, "ledger",
		createdTransactionPayload(2, rebuildTestPosting("orders:1", "world", "USD", 5)),
	)))
	require.NoError(t, batch.SetProto(coldLogKey(5), applyLedgerLog(5, "ledger",
		createdTransactionPayload(3, rebuildTestPosting("world", "orders:1", "USD", 3)),
	)))
	require.NoError(t, batch.SetProto(coldAuditKey(1), auditSuccess(1, 1, 1)))
	require.NoError(t, batch.SetProto(coldAuditKey(2), auditSuccess(2, 2, 2)))
	require.NoError(t, batch.SetProto(coldAuditKey(3), auditSuccess(3, 3, 5)))
	require.NoError(t, batch.Commit())

	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0, 0))

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	attrs := attributes.New()
	pair, err := attrs.Volume.Get(handle, domain.VolumeKey{
		AccountKey: domain.AccountKey{LedgerID: 1, Account: "orders:1"},
		Asset:      "USD",
	}.Bytes())
	require.NoError(t, err)
	require.NotNil(t, pair)
	require.Equal(t, "8", pair.GetInput().ToBigInt().String())
	require.Equal(t, "5", pair.GetOutput().ToBigInt().String())
}
