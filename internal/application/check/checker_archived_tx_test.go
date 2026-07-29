package check

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	domainreplay "github.com/formancehq/ledger/v3/internal/domain/replay"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// A transaction created before the archive boundary (full state in the baseline
// checkpoint, create log purged) and updated afterward — via a metadata set,
// delete, or revert that the replay sees as a partial delta — must not be
// flagged as tampered. The stored state is the correct full state (attribute
// zone, not purged); the checker's expected is baseline + delta, produced by the
// lazy-seed writer seeding the base on the delta's first touch. An unseeded
// replay drops the create data and false-positives — the bug each case pins.
func TestCompareTransactions_ArchivedThenDelta(t *testing.T) {
	t.Parallel()

	postings := []*commonpb.Posting{newPosting("world", "acc", "USD", 100)}
	metaK0 := map[string]*commonpb.MetadataValue{
		"k0": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v0"}},
	}
	metaK2 := map[string]*commonpb.MetadataValue{
		"k2": {Type: &commonpb.MetadataValue_IntValue{IntValue: 42}},
	}
	revertedAt := &commonpb.Timestamp{Data: 111}

	cases := []struct {
		name string
		// live is the full state the store holds after the post-archive delta.
		live *commonpb.TransactionState
		// delta applies the post-archive operation through the replay writer.
		delta func(w domainreplay.Writer, key []byte) error
	}{
		{
			name: "metadata set",
			live: &commonpb.TransactionState{CreatedByLog: 5, Postings: postings, Metadata: map[string]*commonpb.MetadataValue{
				"k0": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v0"}},
				"k2": {Type: &commonpb.MetadataValue_IntValue{IntValue: 42}},
			}},
			delta: func(w domainreplay.Writer, key []byte) error { return w.SaveTxMetadata(key, metaK2) },
		},
		{
			name:  "metadata delete",
			live:  &commonpb.TransactionState{CreatedByLog: 5, Postings: postings},
			delta: func(w domainreplay.Writer, key []byte) error { return w.DeleteTxMetadata(key, "k0") },
		},
		{
			name:  "revert",
			live:  &commonpb.TransactionState{CreatedByLog: 5, Postings: postings, Metadata: metaK0, RevertedByTransaction: 2, RevertedAt: revertedAt},
			delta: func(w domainreplay.Writer, key []byte) error { return w.SetRevertedBy(key, 2, revertedAt) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := logging.Testing()
			meter := noop.NewMeterProvider().Meter("test")
			attrs := attributes.New()

			store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { _ = store.Close() })

			txKey := domain.TransactionKey{LedgerName: "L1", ID: 1}

			// Pre-archive state: created (log 5) with metadata k0. Captured in the baseline.
			batch := store.OpenWriteSession()
			_, err = attrs.Transaction.Set(batch, txKey.Bytes(), &commonpb.TransactionState{
				CreatedByLog: 5,
				Postings:     postings,
				Metadata:     metaK0,
			})
			require.NoError(t, err)
			require.NoError(t, batch.Commit())

			handle, err := store.NewReadHandle()
			require.NoError(t, err)
			baselinePath := filepath.Join(t.TempDir(), "baseline")
			require.NoError(t, attributes.CreateBaselineSnapshot(handle, baselinePath))
			require.NoError(t, handle.Close())

			baselineDB, err := pebble.Open(baselinePath, &pebble.Options{ReadOnly: true})
			require.NoError(t, err)
			t.Cleanup(func() { _ = baselineDB.Close() })

			// Post-archive live/stored state: the full state after the delta.
			batch = store.OpenWriteSession()
			_, err = attrs.Transaction.Set(batch, txKey.Bytes(), tc.live)
			require.NoError(t, err)
			require.NoError(t, batch.Commit())

			checker := NewChecker(store, attrs, "test-cluster", nil, nil, nil, logger)

			// When seeded, the delta is applied through the lazy-seed writer, which
			// seeds the baseline state on first touch — matching production, where
			// ReplayLedgerLog drives the delta through the same writer.
			buildReplay := func(seed bool) *replayStore {
				r := newTestReplayStore(t)

				var w domainreplay.Writer = r
				if seed {
					w = newLazyTxSeedWriter(r, func(canonicalKey []byte) (*commonpb.TransactionState, error) {
						return attrs.Transaction.Get(baselineDB, canonicalKey)
					})
				}
				require.NoError(t, tc.delta(w, txKey.Bytes()))

				return r
			}

			runCompare := func(replay *replayStore) []string {
				reader, err := store.NewReadHandle()
				require.NoError(t, err)
				defer func() { _ = reader.Close() }()

				var msgs []string
				checker.compareTransactions(context.Background(), reader, baselineDB, replay, func(e *servicepb.CheckStoreEvent) {
					if ev := e.GetError(); ev != nil &&
						ev.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH {
						msgs = append(msgs, ev.GetMessage())
					}
				})

				return msgs
			}

			require.NotEmpty(t, runCompare(buildReplay(false)),
				"sanity: an unseeded replay must reproduce the false mismatch")

			require.Empty(t, runCompare(buildReplay(true)),
				"a seeded archived transaction with a post-archive delta must not be flagged as tampered")
		})
	}
}

// A transaction created after the archive boundary is absent from the baseline.
// Its create and delta both flow through the lazy-seed writer; the delta's
// baseline lookup finds nothing and must seed nothing — seeding a nil state
// would append an empty txOpFinalized after the create, resetting away the
// create's fields and flagging the correct live state as tampered.
func TestCompareTransactions_PostArchiveCreateThenDelta(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	meter := noop.NewMeterProvider().Meter("test")
	attrs := attributes.New()

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	// Baseline taken before the transaction exists.
	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	baselinePath := filepath.Join(t.TempDir(), "baseline")
	require.NoError(t, attributes.CreateBaselineSnapshot(handle, baselinePath))
	require.NoError(t, handle.Close())

	baselineDB, err := pebble.Open(baselinePath, &pebble.Options{ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = baselineDB.Close() })

	txKey := domain.TransactionKey{LedgerName: "L1", ID: 1}
	postings := []*commonpb.Posting{newPosting("world", "acc", "USD", 100)}
	ts := &commonpb.Timestamp{Data: 99}
	metaK2 := map[string]*commonpb.MetadataValue{
		"k2": {Type: &commonpb.MetadataValue_IntValue{IntValue: 42}},
	}

	// Live state after create (log 7) + metadata set.
	batch := store.OpenWriteSession()
	_, err = attrs.Transaction.Set(batch, txKey.Bytes(), &commonpb.TransactionState{
		CreatedByLog: 7,
		Timestamp:    ts,
		Postings:     postings,
		Metadata:     metaK2,
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	checker := NewChecker(store, attrs, "test-cluster", nil, nil, nil, logger)

	r := newTestReplayStore(t)
	w := newLazyTxSeedWriter(r, func(canonicalKey []byte) (*commonpb.TransactionState, error) {
		return attrs.Transaction.Get(baselineDB, canonicalKey)
	})
	require.NoError(t, w.CreateTransaction(txKey.Bytes(), 7, ts, nil, postings, 0))
	require.NoError(t, w.SaveTxMetadata(txKey.Bytes(), metaK2))

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()

	var msgs []string
	checker.compareTransactions(context.Background(), reader, baselineDB, r, func(e *servicepb.CheckStoreEvent) {
		if ev := e.GetError(); ev != nil &&
			ev.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH {
			msgs = append(msgs, ev.GetMessage())
		}
	})

	require.Empty(t, msgs,
		"a post-archive transaction with no baseline state must not be flagged as tampered")
}
