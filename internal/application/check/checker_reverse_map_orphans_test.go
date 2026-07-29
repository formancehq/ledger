package check

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// reverseMapFixtureInput describes the two stores the orphan pass compares: the
// primary store's SubAttrIndex registry (the oracle) and the peer read index's
// raw reverse-map rows (the data under judgement).
type reverseMapFixtureInput struct {
	registry map[domain.IndexKey]*commonpb.Index
	// schemas stands in for the Checker's audit-derived expectedSchemas — the
	// replayed metadata schema per ledger, NOT the stored LedgerInfo.
	schemas  map[string]*commonpb.MetadataSchema
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
	schemas map[string]*commonpb.MetadataSchema
}

// run drives the pass on the absence-based oracle only — no replayed
// RemovedMetadataFieldType or DeleteLedger evidence — and collects every emitted
// event, preserving order.
func (f reverseMapFixture) run(lastSequence uint64, live, pendingCleanup map[string]struct{}) []*servicepb.CheckStoreEvent {
	return f.runScope(reverseMapOrphanScope{
		lastSequence:          lastSequence,
		liveLedgers:           live,
		pendingCleanupLedgers: pendingCleanup,
	})
}

// runScope drives the pass with a caller-built scope, for the cases that need
// the positive-evidence oracle terms or a deliberately misaligned peer cursor.
// reader, peer and replayedSchemas always come from the fixture.
func (f reverseMapFixture) runScope(scope reverseMapOrphanScope) []*servicepb.CheckStoreEvent {
	var events []*servicepb.CheckStoreEvent

	scope.reader = f.reader
	scope.replayedSchemas = f.schemas

	if f.checker.readStore != nil {
		scope.peer = f.checker.readStore.NewSnapshot()
		defer func() { _ = scope.peer.Close() }()
	}

	f.checker.compareReverseMapOrphans(scope, func(e *servicepb.CheckStoreEvent) {
		events = append(events, e)
	})

	return events
}

// removedFieldSet builds the replay's positive-evidence oracle: the metadata
// fields a RemovedMetadataFieldType log was observed for.
func removedFieldSet(fields ...schemaField) map[removedSchemaFieldKey]struct{} {
	set := make(map[removedSchemaFieldKey]struct{}, len(fields))
	for _, field := range fields {
		set[removedSchemaFieldKey{ledger: field.ledger, target: field.target, metaKey: field.key}] = struct{}{}
	}

	return set
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
		schemas: in.schemas,
	}
}

// schemaField names one declared metadata field, mirroring what a replayed
// SetMetadataFieldType contributes to the checker's expectedSchemas.
type schemaField struct {
	ledger string
	target commonpb.TargetType
	key    string
}

// replayedSchemas builds the audit-derived schema map through the same helper
// the replay loop uses, so the fixture cannot drift from the real shape.
func replayedSchemas(fields ...schemaField) map[string]*commonpb.MetadataSchema {
	schemas := make(map[string]*commonpb.MetadataSchema)
	for _, field := range fields {
		setExpectedSchemaField(schemas, field.ledger, field.target, field.key, commonpb.MetadataType_METADATA_TYPE_STRING)
	}

	return schemas
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

	// Registry entries with no matching schema field: not a state a real
	// cluster reaches (validateIndexTarget requires SetMetadataFieldType
	// first), but it isolates the registry term of the oracle so this test
	// cannot pass via the schema term instead.
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
// metadata key covered for accounts but not for transactions must leave the
// account rows alone and flag only the transaction rows.
//
// Both terms of the oracle are checked for target-scoping independently: a
// key-only match in either one would wrongly absolve the transaction row.
func TestCompareReverseMapOrphans_IdentityIncludesTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input reverseMapFixtureInput
	}{
		{
			name: "covered by the registry term",
			input: reverseMapFixtureInput{
				registry: metadataRegistry("L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "shared"),
			},
		},
		{
			name: "covered by the schema term",
			input: reverseMapFixtureInput{
				schemas: replayedSchemas(schemaField{"L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "shared"}),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			kb := dal.NewKeyBuilder()

			input := test.input
			input.progress = 3
			input.rmapKeys = [][]byte{
				readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "shared", 1),
				readstore.TransactionReverseMapKeyV(kb, "L1", 9, "shared", 1),
			}

			events := newReverseMapFixture(t, input).run(3, ledgerNameSet("L1"), nil)

			require.Len(t, events, 1)
			require.Contains(t, events[0].GetError().GetMessage(), `namespace "t:"`)
			require.Contains(t, events[0].GetError().GetMessage(), "sample transaction 9")
		})
	}
}

// TestCompareReverseMapOrphans_DropIndexResidueNotFlagged is the regression
// guard for the EN-1621 finding. handleDroppedIndexLog reclaims nothing from
// the read index, and processDropIndex leaves the schema field declared, so a
// dropped metadata index leaves rmap rows behind forever on a healthy cluster.
// A registry-only oracle would report them on every Check() run — permanently
// red on a legitimate operator action, with no warning channel to soften it.
//
// The second half is the sensitivity twin: the same rows with the schema field
// gone MUST be flagged, so the silence above cannot come from an early return.
func TestCompareReverseMapOrphans_DropIndexResidueNotFlagged(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	rows := [][]byte{
		readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "role", 1),
		readstore.AccountReverseMapKeyV(kb, "L1", "users:2", "role", 1),
	}

	dropped := newReverseMapFixture(t, reverseMapFixtureInput{
		// DropIndex removed the registry entry; the schema field survives.
		schemas:  replayedSchemas(schemaField{"L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"}),
		rmapKeys: rows,
		progress: 3,
	})

	require.Empty(t, dropped.run(3, ledgerNameSet("L1"), nil),
		"DropIndex residue must not be flagged: the schema field is still declared (EN-1621)")

	removed := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: rows,
		progress: 3,
	})

	require.Len(t, removed.run(3, ledgerNameSet("L1"), nil), 1,
		"the same rows with the schema field gone must be flagged, proving the scan reached them")
}

// TestCompareReverseMapOrphans_RemovedFieldTypeResidueFlagged is EN-1458's
// target case. RemovedMetadataFieldType is the single log that both removes the
// schema field type and runs purgeReverseMapForKey's point-delete scan, so a
// row whose field is absent from the replayed schema is precisely a row that
// scan missed — the permanent orphan the reverse map's non-range-deletable key
// shape makes possible.
func TestCompareReverseMapOrphans_RemovedFieldTypeResidueFlagged(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		// "kept" survived the removal; "removed" was dropped from the schema by
		// RemovedMetadataFieldType, and its rmap rows should have gone with it.
		registry: metadataRegistry("L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "kept"),
		schemas:  replayedSchemas(schemaField{"L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "kept"}),
		rmapKeys: [][]byte{
			readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "kept", 1),
			readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "removed", 1),
			readstore.AccountReverseMapKeyV(kb, "L1", "users:2", "removed", 1),
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
	require.Contains(t, err.GetMessage(), `"removed"`)
	require.Contains(t, err.GetMessage(), "rows=2")
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

	// A lagging peer must still have its keys decoded: a malformed key is
	// corruption whatever the fold position, and needs no oracle to judge.
	laggingMalformed := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{append([]byte{readstore.PrefixReverseMap}, 0x01, 0x02)},
		progress: 9,
	})

	require.Len(t, laggingMalformed.run(10, ledgerNameSet("L1"), nil), 1,
		"a malformed key needs no oracle, so the lag gate must not suppress it")
}

// TestCompareReverseMapOrphans_CursorAhead pins the other side of the gate.
// Check() pins every oracle term — the index registry, the replayed schemas, the
// live-ledger set — at lastSequence, but the peer read index is folded
// asynchronously on a live node, so by scan time it can hold rows for ledgers and
// fields created AFTER that pin. Judging those by absence reports a healthy
// cluster as corrupt, so the absence-based verdicts require an exactly aligned
// cursor while the positive-evidence ones do not.
func TestCompareReverseMapOrphans_CursorAhead(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	// A metadata field created after the checker's snapshot: the peer folded its
	// rows, but the registry and schema oracles predate it. Absent from both
	// oracle terms, exactly like a removed field — and yet perfectly healthy.
	newField := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "created-later", 1)},
		progress: 12,
	})

	require.Empty(t, newField.run(10, ledgerNameSet("L1"), nil),
		"a field created after the pinned oracle must not be reported as an orphan")

	// A ledger created after the snapshot: same shape, unknown-ledger class.
	newLedger := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{readstore.AccountReverseMapKeyV(kb, "L2", "users:1", "role", 1)},
		progress: 12,
	})

	require.Empty(t, newLedger.run(10, ledgerNameSet("L1"), nil),
		"a ledger created after the pinned oracle must not be reported as absent from the live set")

	// Positive evidence survives the same ahead cursor: the replay saw the
	// removal, so the rows are orphans no matter where the peer cursor sits.
	removed := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "dropped", 1)},
		progress: 12,
	})

	require.Len(t, removed.runScope(reverseMapOrphanScope{
		lastSequence:  10,
		liveLedgers:   ledgerNameSet("L1"),
		removedFields: removedFieldSet(schemaField{"L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "dropped"}),
	}), 1, "an audit-observed removal must be reported even when the peer cursor is ahead")

	// Same for a replayed DeleteLedger.
	deleted := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{readstore.AccountReverseMapKeyV(kb, "gone", "users:1", "role", 1)},
		progress: 12,
	})

	require.Len(t, deleted.runScope(reverseMapOrphanScope{
		lastSequence:   10,
		liveLedgers:    ledgerNameSet("L1"),
		deletedLedgers: ledgerNameSet("gone"),
	}), 1, "an audit-observed DeleteLedger must be reported even when the peer cursor is ahead")
}

// TestCompareReverseMapOrphans_RemovedThenRedeclared pins that the removal set
// being append-only is safe. A field removed and later re-declared by a second
// SetMetadataFieldType has a RemovedMetadataFieldType on record forever, so the
// verdict must consult the replayed schema first or every re-declared field
// would be reported as an orphan.
func TestCompareReverseMapOrphans_RemovedThenRedeclared(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{readstore.AccountReverseMapKeyV(kb, "L1", "users:1", "role", 1)},
		// The re-declaration is what the replayed schema ends up holding.
		schemas:  replayedSchemas(schemaField{"L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"}),
		progress: 10,
	})

	require.Empty(t, fixture.runScope(reverseMapOrphanScope{
		lastSequence:  10,
		liveLedgers:   ledgerNameSet("L1"),
		removedFields: removedFieldSet(schemaField{"L1", commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"}),
	}), "a field re-declared after its removal must not be reported as an orphan")
}

// TestCompareReverseMapOrphans_EmptyAudit pins that an audit with no logs does
// not make the peer store trustworthy. The read index folds FROM the log stream,
// so a reverse-map row over a zero-log store has nothing behind it: it is either
// stale residue or injected. Both are classes this pass reports, and every oracle
// term is legitimately empty at lastSequence 0.
func TestCompareReverseMapOrphans_EmptyAudit(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()

	fixture := newReverseMapFixture(t, reverseMapFixtureInput{
		rmapKeys: [][]byte{
			readstore.AccountReverseMapKeyV(kb, "ghost", "users:1", "role", 1),
			append([]byte{readstore.PrefixReverseMap}, 0x01, 0x02),
		},
		progress: 0,
	})

	events := fixture.run(0, nil, nil)
	require.Len(t, events, 2, "an unaudited row and a malformed key must both be reported over an empty audit")

	messages := []string{events[0].GetError().GetMessage(), events[1].GetError().GetMessage()}
	require.Contains(t, messages[0], "absent from the live ledger set")
	require.Contains(t, messages[1], "do not decode")
}

// TestCheck_ReverseMapOrphans_EmptyAuditWiring is the Check()-level twin of the
// above: it pins that the pass actually runs on the lastSequence == 0 path, which
// returns before the replay and therefore before every other pass.
func TestCheck_ReverseMapOrphans_EmptyAuditWiring(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())
	kb := dal.NewKeyBuilder()

	runCheck := func(t *testing.T, seed bool) []*servicepb.CheckStoreEvent {
		t.Helper()

		peer, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = peer.Close() })

		if seed {
			batch := peer.NewBatch()
			require.NoError(t, batch.SetBytes(readstore.AccountReverseMapKeyV(kb, "ghost", "users:1", "role", 1), []byte{0x01}))
			require.NoError(t, batch.Commit())
		}

		checker := NewChecker(createTestStore(t), attributes.New(), "test-cluster", nil, nil, peer, logger)

		var events []*servicepb.CheckStoreEvent
		require.NoError(t, checker.Check(context.Background(), func(e *servicepb.CheckStoreEvent) {
			if e.GetError() != nil {
				events = append(events, e)
			}
		}))

		return events
	}

	t.Run("stale peer row is reported", func(t *testing.T) {
		t.Parallel()

		events := runCheck(t, true)
		require.Len(t, events, 1, "a reverse-map row with no audit behind it must be reported")
		require.Contains(t, events[0].GetError().GetMessage(), "absent from the live ledger set")
	})

	t.Run("empty peer store stays clean", func(t *testing.T) {
		t.Parallel()

		require.Empty(t, runCheck(t, false),
			"an attached but empty read index over an empty audit must report nothing")
	})
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

// setMetadataFieldTypeOrder builds a real SetMetadataFieldType order, mirroring
// the shape admission.go produces for servicepb.Request_SetMetadataFieldType.
func setMetadataFieldTypeOrder(ledger string, target commonpb.TargetType, key string, typ commonpb.MetadataType) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
						SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{
							TargetType: target,
							Key:        key,
							Type:       typ,
						},
					}},
				},
			},
		},
	}
}

// removeMetadataFieldTypeOrder builds a real RemoveMetadataFieldType order,
// mirroring the shape admission.go produces for
// servicepb.Request_RemoveMetadataFieldType. This is the log EN-1458 targets:
// processRemoveMetadataFieldType both drops the schema field AND (in
// production) triggers the indexbuilder's purgeReverseMapForKey point-delete
// scan of the reverse map.
func removeMetadataFieldTypeOrder(ledger string, target commonpb.TargetType, key string) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{
						RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{
							TargetType: target,
							Key:        key,
						},
					}},
				},
			},
		},
	}
}

// TestCheck_ReverseMapOrphans_EndToEnd proves the pass is actually wired into
// Check(), not merely correct in isolation (every other test in this file
// calls compareReverseMapOrphans directly). It drives real orders through the
// RequestProcessor pipeline — CreateLedger, then SetMetadataFieldType, then
// RemoveMetadataFieldType for the same field — so knownLedgers and
// expectedSchemas are the genuine audit-replayed projections Check() builds,
// not hand-constructed maps.
//
// RemovedMetadataFieldType is the exact log EN-1458 is about: in production it
// both drops the schema field AND runs purgeReverseMapForKey's point-delete
// scan of the reverse map. This test harness has no indexbuilder wired in (its
// testEngine never folds reverse-map rows), so the "scan missed the row" half
// of the bug is reproduced the same way the pass's own unit tests do it: the
// orphaned row is seeded directly into a real peer readstore via its public
// key/write API, standing in for a row the purge scan failed to reach.
func TestCheck_ReverseMapOrphans_EndToEnd(t *testing.T) {
	t.Parallel()

	const ledger = "L1"

	engine := newTestEngine(t)

	engine.processAndCommit(createLedgerOrder(ledger))
	engine.processAndCommit(setMetadataFieldTypeOrder(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role", commonpb.MetadataType_METADATA_TYPE_STRING))
	engine.processAndCommit(removeMetadataFieldTypeOrder(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"))

	logger := logging.Testing()

	peer, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = peer.Close() })

	kb := dal.NewKeyBuilder()
	orphanRow := readstore.AccountReverseMapKeyV(kb, ledger, "users:1", "role", 1)

	batch := peer.NewBatch()
	require.NoError(t, batch.SetBytes(orphanRow, []byte{0x01}))
	// Progress is set far past the store's own last sequence, which does double
	// duty: it keeps the test independent of exactly how many logs the three
	// orders above produced, AND it pins the cursor-ahead case. Detection here
	// therefore rests on the positive-evidence path — the replay saw a
	// RemovedMetadataFieldType for this field — not on the absence-based one,
	// which an unaligned cursor deliberately suppresses. A healthy row under the
	// same ahead cursor must stay silent; that is
	// TestCompareReverseMapOrphans_CursorAhead.
	require.NoError(t, peer.WriteProgress(batch, 1_000_000))
	require.NoError(t, batch.Commit())

	checker := NewChecker(engine.store, engine.attrs, engine.clusterID, nil, nil, peer, logger)

	var events []*servicepb.CheckStoreEvent
	require.NoError(t, checker.Check(context.Background(), func(e *servicepb.CheckStoreEvent) {
		if e.GetError() != nil {
			events = append(events, e)
		}
	}))

	require.Len(t, events, 1, "the only integrity error in this store must be the reverse-map orphan")

	err0 := events[0].GetError()
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERSE_MAP_ORPHAN, err0.GetErrorType())
	require.Equal(t, ledger, err0.GetLedger())
	require.Contains(t, err0.GetMessage(), `"role"`)
}
