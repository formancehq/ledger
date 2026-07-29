package check

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// reverseMapFixtureInput describes the two stores the orphan pass compares: the
// primary store's SubAttrIndex registry (the oracle) and the peer read index's
// raw reverse-map rows (the data under judgement).
type reverseMapFixtureInput struct {
	registry map[domain.IndexKey]*commonpb.Index
	rmapKeys [][]byte
	// progress is the read index's last-folded log sequence, written to the
	// progress cursor so the lag gate can be driven from a test.
	progress uint64
	// withoutReadStore builds a Checker with no peer read index, as the
	// restore / CLI call sites do.
	withoutReadStore bool
}

// reverseMapFixture holds a wired Checker plus the pinned primary-store reader
// the pass needs.
type reverseMapFixture struct {
	checker *Checker
	reader  dal.PebbleReader
}

// run drives the pass and collects every emitted event, preserving order.
func (f reverseMapFixture) run(lastSequence uint64, live, pendingCleanup map[string]struct{}) []*servicepb.CheckStoreEvent {
	var events []*servicepb.CheckStoreEvent

	f.checker.compareReverseMapOrphans(f.reader, lastSequence, live, pendingCleanup, func(e *servicepb.CheckStoreEvent) {
		events = append(events, e)
	})

	return events
}

func newReverseMapFixture(t *testing.T, in reverseMapFixtureInput) reverseMapFixture {
	t.Helper()

	store := createTestStore(t)
	attrs := attributes.New()

	if len(in.registry) > 0 {
		batch := store.OpenWriteSession()
		for key, idx := range in.registry {
			_, err := attrs.Index.Set(batch, key.Bytes(), idx)
			require.NoError(t, err)
		}

		require.NoError(t, batch.Commit())
	}

	logger := logging.FromContext(logging.TestingContext())

	var peer *readstore.Store

	if !in.withoutReadStore {
		var err error

		peer, err = readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = peer.Close() })

		batch := peer.NewBatch()
		for _, key := range in.rmapKeys {
			// The real writer stores the encoded metadata value here; the pass
			// only reads keys, so any non-empty value will do.
			require.NoError(t, batch.SetBytes(key, []byte{0x01}))
		}

		require.NoError(t, peer.WriteProgress(batch, in.progress))
		require.NoError(t, batch.Commit())
	}

	reader, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Close() })

	return reverseMapFixture{
		checker: NewChecker(store, attrs, "test-cluster", nil, nil, peer, logger),
		reader:  reader,
	}
}

// metadataRegistry builds registry rows for (ledger, target, key) triples.
func metadataRegistry(ledger string, target commonpb.TargetType, keys ...string) map[domain.IndexKey]*commonpb.Index {
	registry := make(map[domain.IndexKey]*commonpb.Index, len(keys))

	for _, key := range keys {
		id := indexes.MetadataID(target, key)
		registry[indexes.KeyFor(ledger, id)] = &commonpb.Index{Id: id, Ledger: ledger}
	}

	return registry
}

func ledgerNameSet(names ...string) map[string]struct{} {
	set := make(map[string]struct{}, len(names))
	for _, name := range names {
		set[name] = struct{}{}
	}

	return set
}

// TestCompareReverseMapOrphans_IndexedFieldsStaySilent pins the happy path AND
// proves the silence is earned: the twin sub-case reuses the exact same rows
// with an empty registry and must flag them. Without that twin, a pass that
// returned early (nil read store, closed lag gate, empty scan) would produce
// the same "no events" result.
func TestCompareReverseMapOrphans_IndexedFieldsStaySilent(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	rows := [][]byte{
		readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "role", 1),
		readstore.TransactionReverseMapKeyV(kb, "L1", 7, "tier", 1),
	}

	full := newReverseMapFixture(t, reverseMapFixtureInput{
		registry: mergeRegistries(
			metadataRegistry("L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"),
			metadataRegistry("L1", commonpb.TargetType_TARGET_TYPE_TRANSACTION, "tier"),
		),
		rmapKeys: rows,
		progress: 10,
	})

	require.Empty(t, full.run(10, ledgerNameSet("L1"), nil),
		"every row's field is indexed: the pass must stay silent")

	bare := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: rows,
		progress: 10,
	})

	require.Len(t, bare.run(10, ledgerNameSet("L1"), nil), 2,
		"the same rows with an empty registry must be flagged, proving the scan reached them")
}

func mergeRegistries(sets ...map[domain.IndexKey]*commonpb.Index) map[domain.IndexKey]*commonpb.Index {
	merged := make(map[domain.IndexKey]*commonpb.Index)
	for _, set := range sets {
		maps.Copy(merged, set)
	}

	return merged
}

// TestCompareReverseMapOrphans_IgnoresVersion pins the version-agnostic
// decision: current and pending forward-encoding versions legitimately coexist
// during a per-replica rewrite, so rows at two different versions for a
// still-indexed field are not orphans.
func TestCompareReverseMapOrphans_IgnoresVersion(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		registry: metadataRegistry("L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"),
		rmapKeys: [][]byte{
			readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "role", 1),
			readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "role", 2),
			readstore.AccountReverseMapKeyV(kb, "L1", "users:2", "role", 7),
		},
		progress: 4,
	})

	require.Empty(t, fixture.run(4, ledgerNameSet("L1"), nil),
		"a row at any version whose field is still indexed is not an orphan")
}

// TestCompareReverseMapOrphans_AccountOrphan covers the core detection: rows
// for an account metadata field with no registry entry.
func TestCompareReverseMapOrphans_AccountOrphan(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		registry: metadataRegistry("L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"),
		rmapKeys: [][]byte{
			readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "role", 1),
			readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "dropped", 1),
		},
		progress: 3,
	})

	events := fixture.run(3, ledgerNameSet("L1"), nil)

	require.Len(t, events, 1)
	err := events[0].GetError()
	require.Equal(t,
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
		err.GetErrorType())
	require.Equal(t, "L1", err.GetLedger())
	require.Contains(t, err.GetMessage(), `"dropped"`)
	require.Contains(t, err.GetMessage(), `namespace "a:"`)
	require.Contains(t, err.GetMessage(), "rows=1")
	require.Contains(t, err.GetMessage(), "sample account users:1")
}

// TestCompareReverseMapOrphans_TransactionOrphan covers the transaction
// namespace and pins the decimal txID rendering of the sample entity.
func TestCompareReverseMapOrphans_TransactionOrphan(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{
			readstore.TransactionReverseMapKeyV(kb, "L1", 42, "dropped", 1),
		},
		progress: 3,
	})

	events := fixture.run(3, ledgerNameSet("L1"), nil)

	require.Len(t, events, 1)
	require.Contains(t, events[0].GetError().GetMessage(), `namespace "t:"`)
	require.Contains(t, events[0].GetError().GetMessage(), "sample transaction 42")
}

// TestCompareReverseMapOrphans_AggregatesPerField pins aggregation: a field
// dropped on a large ledger strands many rows, and the pass must report one
// event carrying the row count rather than one event per row.
func TestCompareReverseMapOrphans_AggregatesPerField(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	var rows [][]byte
	for _, account := range []string{"users:1", "users:2", "users:3", "users:4", "users:5"} {
		rows = append(rows, readstore.AccountReverseMapKeyV(kb, "L1", account, "dropped", 1))
	}

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: rows,
		progress: 3,
	})

	events := fixture.run(3, ledgerNameSet("L1"), nil)

	require.Len(t, events, 1, "five stranded rows for one field must produce exactly one event")
	require.Contains(t, events[0].GetError().GetMessage(), "rows=5")
	require.Contains(t, events[0].GetError().GetMessage(), "sample account users:1",
		"the sample must be the first row in key order, so the report is deterministic")
}

// TestCompareReverseMapOrphans_IdentityIncludesTarget pins that a row's
// identity is (ledger, target, metadata key) — never the key alone. The same
// metadata key indexed for accounts but not for transactions must leave the
// account rows alone and flag only the transaction rows.
func TestCompareReverseMapOrphans_IdentityIncludesTarget(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		registry: metadataRegistry("L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "shared"),
		rmapKeys: [][]byte{
			readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "shared", 1),
			readstore.TransactionReverseMapKeyV(kb, "L1", 9, "shared", 1),
		},
		progress: 3,
	})

	events := fixture.run(3, ledgerNameSet("L1"), nil)

	require.Len(t, events, 1)
	require.Contains(t, events[0].GetError().GetMessage(), `namespace "t:"`)
	require.Contains(t, events[0].GetError().GetMessage(), "sample transaction 9")
}

// TestCompareReverseMapOrphans_UnknownLedger covers the leak class:
// DeleteLedger range-deletes the whole [0x03][ledger] span unconditionally, so
// with the lag gate satisfied a surviving row for an unknown ledger is real.
func TestCompareReverseMapOrphans_UnknownLedger(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{
			readstore.AccountReverseMapKeyV(kb, "gone", "users:1", "role", 1),
			readstore.AccountReverseMapKeyV(kb, "gone", "users:2", "tier", 1),
		},
		progress: 3,
	})

	events := fixture.run(3, ledgerNameSet("L1"), nil)

	require.Len(t, events, 1, "rows for an unknown ledger aggregate into one per-ledger event")
	require.Equal(t, "gone", events[0].GetError().GetLedger())
	require.Contains(t, events[0].GetError().GetMessage(), "absent from the live ledger set")
	require.Contains(t, events[0].GetError().GetMessage(), "rows=2")
}

// TestCompareReverseMapOrphans_PendingCleanupSkipped covers the deferred-purge
// window: a deleted ledger's rows legitimately linger until a chapter purge
// catches the DeleteLedger sequence, exactly as the sibling passes tolerate.
func TestCompareReverseMapOrphans_PendingCleanupSkipped(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{
			readstore.AccountReverseMapKeyV(kb, "draining", "users:1", "role", 1),
		},
		progress: 3,
	})

	require.Empty(t, fixture.run(3, ledgerNameSet("L1"), ledgerNameSet("draining")),
		"rows of a cleanup-pending ledger must not be flagged")
}

// TestCompareReverseMapOrphans_MalformedKeys pins that a key the chokepoint
// rejects is reported with the specific sentinel that failed, so the operator
// learns which corruption they are looking at.
func TestCompareReverseMapOrphans_MalformedKeys(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	tests := []struct {
		name           string
		key            []byte
		expectedLedger string
		expectedCause  string
	}{
		{
			// A NUL inside the metadata key means the entity/version split
			// landed in the wrong place — the shape check that turns a silent
			// mis-decode into a surfaced error.
			name:           "nul in metadata key",
			key:            readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "ro\x00le", 1),
			expectedLedger: "L1",
			expectedCause:  readstore.ErrReverseMapKeyMetadataKey.Error(),
		},
		{
			// Too short to even hold the fixed-width ledger name block, so the
			// finding cannot be attributed to a ledger.
			name:           "truncated header",
			key:            []byte{readstore.PrefixReverseMap, 'L'},
			expectedLedger: "",
			expectedCause:  readstore.ErrReverseMapKeyTruncated.Error(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := newReverseMapFixture(t, reverseMapFixtureInput{
				rmapKeys: [][]byte{test.key},
				progress: 3,
			})

			events := fixture.run(3, ledgerNameSet("L1"), nil)

			require.Len(t, events, 1)
			err := events[0].GetError()
			require.Equal(t,
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN,
				err.GetErrorType())
			require.Equal(t, test.expectedLedger, err.GetLedger())
			require.Contains(t, err.GetMessage(), "do not decode")
			require.Contains(t, err.GetMessage(), test.expectedCause)
		})
	}
}

// TestCompareReverseMapOrphans_BucketScopedRegistryIgnored pins that a
// bucket-scoped registry row (empty ledger name) never satisfies a
// ledger-scoped reverse-map row: an rmap key always carries a ledger name, so
// the two can't legitimately match.
func TestCompareReverseMapOrphans_BucketScopedRegistryIgnored(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		registry: map[domain.IndexKey]*commonpb.Index{
			indexes.KeyFor("", id): {Id: id},
		},
		rmapKeys: [][]byte{
			readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "role", 1),
		},
		progress: 3,
	})

	events := fixture.run(3, ledgerNameSet("L1"), nil)

	require.Len(t, events, 1, "a bucket-scoped registry row must not cover a ledger-scoped rmap row")
	require.Equal(t, "L1", events[0].GetError().GetLedger())
}

// TestCompareReverseMapOrphans_NoReadStore covers the restore / CLI call sites:
// there is no peer read index to verify, so the pass skips with a log rather
// than reporting a clean result it never checked.
func TestCompareReverseMapOrphans_NoReadStore(t *testing.T) {
	t.Parallel()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{withoutReadStore: true})

	require.Empty(t, fixture.run(3, ledgerNameSet("L1"), nil),
		"with no peer read index the pass must emit nothing")
}

// TestCompareReverseMapOrphans_LagGate pins the load-bearing gate: the registry
// is written at Raft apply while the rmap is folded asynchronously, so between
// apply and fold a legitimately-removed field has no registry entry but live
// rmap rows. Without the gate the pass false-positives on every healthy
// cluster mid-fold.
func TestCompareReverseMapOrphans_LagGate(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	rows := [][]byte{
		readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "dropped", 1),
	}

	lagging := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: rows,
		progress: 9,
	})

	require.Empty(t, lagging.run(10, ledgerNameSet("L1"), nil),
		"a read index behind the verified range must not be judged")

	caughtUp := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: rows,
		progress: 10,
	})

	require.Len(t, caughtUp.run(10, ledgerNameSet("L1"), nil), 1,
		"the same rows must be flagged once the read index has caught up")
}

// TestCompareReverseMapOrphans_DeterministicOrdering pins that the emitted
// event stream is stable across runs: aggregates are emitted in sorted order,
// not in Go map iteration order.
func TestCompareReverseMapOrphans_DeterministicOrdering(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{
			readstore.AccountReverseMapKeyV(kb, "Lc", "users:1", "gamma", 1),
			readstore.AccountReverseMapKeyV(kb, "La", "users:1", "alpha", 1),
			readstore.AccountReverseMapKeyV(kb, "Lb", "users:1", "beta", 1),
			readstore.AccountReverseMapKeyV(kb, "La", "users:1", "delta", 1),
		},
		progress: 3,
	})

	live := ledgerNameSet("La", "Lb", "Lc")

	first := fixture.run(3, live, nil)
	require.Len(t, first, 4)

	ledgers := make([]string, 0, len(first))
	messages := make([]string, 0, len(first))

	for _, event := range first {
		ledgers = append(ledgers, event.GetError().GetLedger())
		messages = append(messages, event.GetError().GetMessage())
	}

	require.Equal(t, []string{"La", "La", "Lb", "Lc"}, ledgers)

	second := fixture.run(3, live, nil)
	require.Len(t, second, 4)

	replayed := make([]string, 0, len(second))
	for _, event := range second {
		replayed = append(replayed, event.GetError().GetMessage())
	}

	require.Equal(t, messages, replayed, "two runs over the same store must emit an identical stream")
}
