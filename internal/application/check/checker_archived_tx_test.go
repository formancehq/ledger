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
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// A transaction created before the archive boundary (full state in the baseline
// checkpoint, create log purged) and updated afterward (a metadata set the
// replay sees as a partial delta) must not be flagged as tampered. The stored
// state is the correct full state (attribute zone, not purged); the checker's
// expected must be baseline + delta. Using the partial replay alone drops the
// create data and false-positives — the bug this pins.
func TestCompareTransactions_ArchivedThenUpdated(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	meter := noop.NewMeterProvider().Meter("test")
	attrs := attributes.New()

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const ledger = "L1"
	txKey := domain.TransactionKey{LedgerName: ledger, ID: 1}
	postings := []*commonpb.Posting{newPosting("world", "acc", "USD", 100)}
	metaK0 := map[string]*commonpb.MetadataValue{
		"k0": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v0"}},
	}
	metaK2 := map[string]*commonpb.MetadataValue{
		"k2": {Type: &commonpb.MetadataValue_IntValue{IntValue: 42}},
	}

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

	// Post-archive update: metadata k2 is set. The live/stored state is the full
	// merged state (k0 from the create + k2 from the update).
	batch = store.OpenWriteSession()
	_, err = attrs.Transaction.Set(batch, txKey.Bytes(), &commonpb.TransactionState{
		CreatedByLog: 5,
		Postings:     postings,
		Metadata: map[string]*commonpb.MetadataValue{
			"k0": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v0"}},
			"k2": {Type: &commonpb.MetadataValue_IntValue{IntValue: 42}},
		},
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	checker := NewChecker(store, attrs, "test-cluster", nil, nil, logger)

	// A replay that saw ONLY the post-archive k2 set — the create log was purged.
	buildReplay := func() *replayStore {
		r := newTestReplayStore(t)
		require.NoError(t, r.SaveTxMetadata(txKey.Bytes(), metaK2))

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

	// Bug: with only the partial delta, the replay overrides the baseline create
	// data, so the correct live state looks tampered.
	require.NotEmpty(t, runCompare(buildReplay()),
		"sanity: an unseeded replay must reproduce the false mismatch")

	// Fix: seeding the baseline transaction state makes the delta merge on top,
	// so expected == live and nothing is flagged.
	seeded := buildReplay()
	require.NoError(t, checker.seedReplayTransactionsFromBaseline(baselineDB, seeded))
	require.Empty(t, runCompare(seeded),
		"an archived transaction updated after the boundary must not be flagged as tampered")
}

// After archive -> DeleteLedger -> same-name recreate, transaction ids restart
// at 1. The recreated tx's create op must reset the seeded (old-generation)
// baseline state; otherwise the old generation's reverted_by / reverted_at (and
// any absent metadata / nil timestamp) leak into the new state and produce a
// false TRANSACTION_UPDATE_MISMATCH against the correct live state.
func TestCompareTransactions_ArchivedThenDeletedAndRecreated(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	meter := noop.NewMeterProvider().Meter("test")
	attrs := attributes.New()

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	const ledger = "L1"
	txKey := domain.TransactionKey{LedgerName: ledger, ID: 1}
	newTs := &commonpb.Timestamp{Data: 1700000000000000}
	newMeta := map[string]*commonpb.MetadataValue{"k1": {Type: &commonpb.MetadataValue_StringValue{StringValue: "new"}}}
	newPostings := []*commonpb.Posting{newPosting("world", "acc", "USD", 50)}

	// Pre-archive OLD generation of tx 1: created (log 5), reverted by tx 2, with
	// metadata k0. Captured in the baseline.
	batch := store.OpenWriteSession()
	_, err = attrs.Transaction.Set(batch, txKey.Bytes(), &commonpb.TransactionState{
		CreatedByLog:          5,
		Postings:              []*commonpb.Posting{newPosting("world", "acc", "USD", 100)},
		Metadata:              map[string]*commonpb.MetadataValue{"k0": {Type: &commonpb.MetadataValue_StringValue{StringValue: "old"}}},
		RevertedByTransaction: 2,
		RevertedAt:            &commonpb.Timestamp{Data: 111},
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

	// After delete + same-name recreate, txID 1 is reused for a fresh, unreverted
	// transaction. The live/stored state is the NEW generation only.
	newState := &commonpb.TransactionState{
		CreatedByLog: 10,
		Postings:     newPostings,
		Timestamp:    newTs,
		Metadata:     newMeta,
	}
	batch = store.OpenWriteSession()
	_, err = attrs.Transaction.Set(batch, txKey.Bytes(), newState)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	checker := NewChecker(store, attrs, "test-cluster", nil, nil, logger)

	// Replay: seed the baseline (old gen), then the recreated ledger's create for
	// the reused txID.
	replay := newTestReplayStore(t)
	require.NoError(t, checker.seedReplayTransactionsFromBaseline(baselineDB, replay))
	require.NoError(t, replay.CreateTransaction(txKey.Bytes(), 10, newTs, newMeta, newPostings, 0))

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	var msgs []string
	checker.compareTransactions(context.Background(), reader, baselineDB, replay, func(e *servicepb.CheckStoreEvent) {
		if ev := e.GetError(); ev != nil &&
			ev.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH {
			msgs = append(msgs, ev.GetMessage())
		}
	})

	require.Empty(t, msgs,
		"a reused txID after delete+recreate must reset the seeded base, not inherit old-generation reverted/metadata state")
}
