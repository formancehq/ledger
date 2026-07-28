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

	// A replay of the post-archive k2 set. When seeded, the delta is applied
	// through the lazy-seed writer, which seeds the baseline state on first touch —
	// matching production, where ReplayLedgerLog drives the delta through the same
	// writer, so the merger sees the base operand before the delta.
	buildReplay := func(seed bool) *replayStore {
		r := newTestReplayStore(t)

		var w domainreplay.Writer = r
		if seed {
			w = newLazyTxSeedWriter(r, func(canonicalKey []byte) (*commonpb.TransactionState, error) {
				return attrs.Transaction.Get(baselineDB, canonicalKey)
			})
		}
		require.NoError(t, w.SaveTxMetadata(txKey.Bytes(), metaK2))

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
	require.NotEmpty(t, runCompare(buildReplay(false)),
		"sanity: an unseeded replay must reproduce the false mismatch")

	// Fix: seeding the baseline transaction state before the delta makes the delta
	// merge on top, so expected == live and nothing is flagged.
	require.Empty(t, runCompare(buildReplay(true)),
		"an archived transaction updated after the boundary must not be flagged as tampered")
}
