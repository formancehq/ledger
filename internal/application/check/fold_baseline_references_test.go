package check

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// openBaselineWithReferences builds a real Pebble baseline seeded with the
// given (ledger, reference) → owning-tx-id TransactionReference attribute
// entries, exactly as the production baseline checkpoint would carry them.
// It writes through a dal.Store (so the on-disk key layout matches what
// foldBaselineReferences reads back), snapshots it, and reopens the
// checkpoint read-only as a standalone *pebble.DB — the same shape the real
// Check() path passes to foldBaselineReferences.
func openBaselineWithReferences(t *testing.T, attrs *attributes.Attributes, entries map[domain.TransactionReferenceKey]uint64) *pebble.DB {
	t.Helper()

	logger := logging.Testing()
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	batch := store.OpenWriteSession()
	for key, txID := range entries {
		_, err := attrs.References.Set(batch, key.Bytes(), &commonpb.TransactionReferenceValue{
			TransactionId: txID,
		})
		require.NoError(t, err)
	}
	require.NoError(t, batch.Commit())

	checkpointID, err := store.CreateSnapshot()
	require.NoError(t, err)

	checkpointPath := filepath.Join(store.DataDir(), "checkpoints", strconv.FormatUint(checkpointID, 10))
	db, err := pebble.Open(checkpointPath, &pebble.Options{ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return db
}

// TestFoldBaselineReferences_SeedsSeqAndTxID exercises the produce side of
// foldBaselineReferences directly: it builds a real Pebble baseline with two
// TransactionReference entries and asserts the fold (a) returns (true, nil),
// (b) seeds each reference with the sentinel sequence 0 in the references
// map, and (c) folds each entry's owning transaction id into referenceTxIDs.
// This closes finding EN-1356 #6 (foldBaselineReferences was previously only
// exercised indirectly via hand-seeded maps) and pins the tx-id fold added
// for the existingTransactionId verification (invariant #8).
func TestFoldBaselineReferences_SeedsSeqAndTxID(t *testing.T) {
	t.Parallel()

	attrs := attributes.New()
	baselineDB := openBaselineWithReferences(t, attrs, map[domain.TransactionReferenceKey]uint64{
		{LedgerName: "L", Reference: "ref-a"}: 42,
		{LedgerName: "L", Reference: "ref-b"}: 7,
	})

	checker := NewChecker(nil, attrs, "test-cluster", logging.Testing())

	references := make(map[string]map[string]uint64)
	referenceTxIDs := make(map[string]map[string]uint64)

	loaded, err := checker.foldBaselineReferences(baselineDB, references, referenceTxIDs)
	require.NoError(t, err)
	require.True(t, loaded, "baseline was available and iterated → must report loaded=true")

	// Sentinel-0 seeding: every archived reference precedes any live log seq.
	require.Equal(t, uint64(0), references["L"]["ref-a"])
	require.Equal(t, uint64(0), references["L"]["ref-b"])

	// Owning tx ids folded from TransactionReferenceValue.transaction_id.
	require.Equal(t, uint64(42), referenceTxIDs["L"]["ref-a"])
	require.Equal(t, uint64(7), referenceTxIDs["L"]["ref-b"])
}

// TestFoldBaselineReferences_NilBaselineShortCircuits pins the (false, nil)
// contract when no baseline is available — the caller uses the bool to scope
// the archive escape, so a nil DB must not report the fold as loaded.
func TestFoldBaselineReferences_NilBaselineShortCircuits(t *testing.T) {
	t.Parallel()

	checker := NewChecker(nil, attributes.New(), "test-cluster", logging.Testing())

	references := make(map[string]map[string]uint64)
	referenceTxIDs := make(map[string]map[string]uint64)

	loaded, err := checker.foldBaselineReferences(nil, references, referenceTxIDs)
	require.NoError(t, err)
	require.False(t, loaded)
	require.Empty(t, references)
	require.Empty(t, referenceTxIDs)
}
