package backup

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
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

func TestRebuildDelta_CleanEOFSucceeds(t *testing.T) {
	t.Parallel()

	store := newRebuildTestStore(t)

	batch := store.NewBatch()
	for seq := uint64(1); seq <= 3; seq++ {
		require.NoError(t, batch.SetProto(coldLogKey(seq), createLedgerLog(seq, "ledger", uint32(seq))))
	}
	require.NoError(t, batch.Commit())

	// A clean stream must terminate via io.EOF and report success.
	require.NoError(t, RebuildDelta(context.Background(), testLogger(), store, 0))

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

	err := RebuildDelta(context.Background(), testLogger(), store, 0)

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
