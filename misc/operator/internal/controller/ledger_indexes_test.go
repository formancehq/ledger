package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// ---------------------------------------------------------------------------
// desiredIndexes — flattening + CRD→CLI mapping
// ---------------------------------------------------------------------------

func TestDesiredIndexes_Nil(t *testing.T) {
	t.Parallel()

	assert.Nil(t, desiredIndexes(nil))
}

func TestDesiredIndexes_Empty(t *testing.T) {
	t.Parallel()

	// A present-but-empty spec is "managed with zero indexes" — non-nil input,
	// empty result.
	assert.Empty(t, desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{}))
}

func TestDesiredIndexes_AllKinds(t *testing.T) {
	t.Parallel()

	got := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{
		Transaction: []string{"reference", "sourceAddress", "destinationAddress", "insertedAt", "revertedAt"},
		Account:     []string{"asset"},
		Metadata: []ledgerv1alpha1.MetadataIndexSpec{
			{Target: "account", Key: "category", Type: "string"},
			{Target: "transaction", Key: "priority", Type: "int64"},
		},
	})

	assert.ElementsMatch(t, []string{
		"reference", "source-address", "destination-address", "inserted-at", "reverted-at",
		"account-asset",
		"metadata:account:category", "metadata:transaction:priority",
	}, canonicalList(got))

	// The CLI invocation fields must be correct for a metadata index.
	var meta managedIndex
	for _, mi := range got {
		if mi.canonical == "metadata:account:category" {
			meta = mi
		}
	}
	require.Equal(t, metadataTypeFlag, meta.typeFlag)
	assert.Equal(t, "account", meta.target)
	assert.Equal(t, "category", meta.key)
	assert.Equal(t, "string", meta.mdType)
}

func TestDesiredIndexes_SkipsUnknownAndDuplicates(t *testing.T) {
	t.Parallel()

	got := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{
		Transaction: []string{"reference", "reference", "bogus"},
		Account:     []string{"asset", "unknown"},
		Metadata: []ledgerv1alpha1.MetadataIndexSpec{
			{Target: "ledger", Key: "k", Type: "string"}, // ledger target unrepresentable
			{Target: "account", Key: "", Type: "string"}, // empty key
			{Target: "account", Key: "k", Type: "bogus"}, // bad type
		},
	})

	assert.ElementsMatch(t, []string{"reference", "account-asset"}, canonicalList(got))
}

// ---------------------------------------------------------------------------
// createArgs / dropArgs / setMetadataTypeArgs
// ---------------------------------------------------------------------------

func TestManagedIndex_Args_Builtin(t *testing.T) {
	t.Parallel()

	mi := managedIndex{canonical: "reference", typeFlag: "reference"}
	assert.Equal(t, []string{"indexes", "create", "--ledger", "L", "--type", "reference"}, mi.createArgs("L"))
	assert.Equal(t, []string{"indexes", "drop", "--ledger", "L", "--type", "reference"}, mi.dropArgs("L"))
}

func TestManagedIndex_Args_Metadata(t *testing.T) {
	t.Parallel()

	mi := managedIndex{
		canonical: "metadata:account:category",
		typeFlag:  metadataTypeFlag,
		target:    "account",
		key:       "category",
		mdType:    "string",
	}
	assert.Equal(t, []string{"indexes", "create", "--ledger", "L", "--type", "metadata", "--target", "account", "--key", "category"}, mi.createArgs("L"))
	assert.Equal(t, []string{"indexes", "drop", "--ledger", "L", "--type", "metadata", "--target", "account", "--key", "category"}, mi.dropArgs("L"))
	assert.Equal(t, []string{"ledgers", "set-metadata-type", "--ledger", "L", "--target", "account", "--key", "category", "--type", "string"}, mi.setMetadataTypeArgs("L"))
}

// ---------------------------------------------------------------------------
// canonical round-trip
// ---------------------------------------------------------------------------

func TestCanonicalRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct{ typeFlag, target, key string }{
		{"reference", "", ""},
		{"account-asset", "", ""},
		{"metadata", "account", "category"},
		{"metadata", "transaction", "weird:key:with:colons"},
	}

	for _, c := range cases {
		canonical := canonicalIndex(c.typeFlag, c.target, c.key)
		gotType, gotTarget, gotKey := parseCanonicalIndex(canonical)
		assert.Equal(t, c.typeFlag, gotType, canonical)
		assert.Equal(t, c.target, gotTarget, canonical)
		assert.Equal(t, c.key, gotKey, canonical)
	}
}

// ---------------------------------------------------------------------------
// parseActualIndexes — protojson shape of `indexes list --json`
// ---------------------------------------------------------------------------

func TestParseActualIndexes_Empty(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "  ", "[]", "[]\n"} {
		got, err := parseActualIndexes(in)
		require.NoError(t, err, in)
		assert.Empty(t, got, in)
	}
}

func TestParseActualIndexes_AllKinds(t *testing.T) {
	t.Parallel()

	// Shape mirrors protojson of []*commonpb.Index: oneof member inlined under
	// its camelCase name, enums as string names, EmitUnpopulated fills scalars.
	const out = `[
	  {"id": {"txBuiltin": "TX_BUILTIN_INDEX_REFERENCE"}, "buildStatus": "INDEX_BUILD_STATUS_READY", "ledger": "L", "forwardEncodingVersion": 1},
	  {"id": {"txBuiltin": "TX_BUILTIN_INDEX_SOURCE_ADDRESS"}, "ledger": "L"},
	  {"id": {"accountBuiltin": "ACCT_BUILTIN_INDEX_ASSET"}, "ledger": "L"},
	  {"id": {"metadata": {"target": "TARGET_TYPE_ACCOUNT", "key": "category"}}, "ledger": "L"},
	  {"id": {"metadata": {"target": "TARGET_TYPE_TRANSACTION", "key": "priority"}}, "ledger": "L"}
	]`

	got, err := parseActualIndexes(out)
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{
		"reference":                     true,
		"source-address":                true,
		"account-asset":                 true,
		"metadata:account:category":     true,
		"metadata:transaction:priority": true,
	}, got)
}

func TestParseActualIndexes_SkipsUnrepresentable(t *testing.T) {
	t.Parallel()

	// The ID tx-builtin, log builtins, ledger-target metadata, and unknown
	// account builtins are all CRD-unrepresentable: they must never appear as
	// candidates (so they are never dropped).
	const out = `[
	  {"id": {"txBuiltin": "TX_BUILTIN_INDEX_ID"}, "ledger": "L"},
	  {"id": {"logBuiltin": "LOG_BUILTIN_INDEX_DATE"}, "ledger": "L"},
	  {"id": {"metadata": {"target": "TARGET_TYPE_LEDGER", "key": "env"}}, "ledger": "L"},
	  {"id": {"accountBuiltin": "ACCT_BUILTIN_INDEX_UNSPECIFIED"}, "ledger": "L"},
	  {"id": {"txBuiltin": "TX_BUILTIN_INDEX_REFERENCE"}, "ledger": "L"}
	]`

	got, err := parseActualIndexes(out)
	require.NoError(t, err)
	assert.Equal(t, map[string]bool{"reference": true}, got)
}

func TestParseActualIndexes_BadJSON(t *testing.T) {
	t.Parallel()

	_, err := parseActualIndexes("not json")
	require.Error(t, err)
}

// ---------------------------------------------------------------------------
// parseSchema + metadataFieldNeedsUpdate
// ---------------------------------------------------------------------------

func TestMetadataFieldNeedsUpdate(t *testing.T) {
	t.Parallel()

	const out = `{
	  "accountFields": {"category": {"declaredType": "METADATA_TYPE_STRING"}},
	  "transactionFields": {"priority": {"declaredType": "METADATA_TYPE_INT64"}}
	}`

	schema, err := parseSchema(out)
	require.NoError(t, err)

	// Absent field → needs declaring.
	assert.True(t, metadataFieldNeedsUpdate(schema, managedIndex{target: "account", key: "missing", mdType: "string"}))
	// Present, matching type → no update.
	assert.False(t, metadataFieldNeedsUpdate(schema, managedIndex{target: "account", key: "category", mdType: "string"}))
	// Present, different type → needs re-declaring (type change is reconciled,
	// not silently ignored).
	assert.True(t, metadataFieldNeedsUpdate(schema, managedIndex{target: "account", key: "category", mdType: "int64"}))
	assert.False(t, metadataFieldNeedsUpdate(schema, managedIndex{target: "transaction", key: "priority", mdType: "int64"}))
}

func TestParseSchema_Empty(t *testing.T) {
	t.Parallel()

	schema, err := parseSchema("")
	require.NoError(t, err)
	_, ok := schema.declaredType("account", "x")
	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// diffIndexes — ownership-scoped create/drop
// ---------------------------------------------------------------------------

func canonicalsOf(indexes []managedIndex) []string {
	out := make([]string, 0, len(indexes))
	for _, mi := range indexes {
		out = append(out, mi.canonical)
	}

	return out
}

func TestDiffIndexes_AddOnly(t *testing.T) {
	t.Parallel()

	desired := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference", "timestamp"}})
	diff := diffIndexes(desired, map[string]bool{"reference": true}, nil)

	assert.ElementsMatch(t, []string{"timestamp"}, canonicalsOf(diff.toCreate))
	assert.Empty(t, diff.toDrop)
}

func TestDiffIndexes_DropOnly(t *testing.T) {
	t.Parallel()

	// Previously applied "timestamp" no longer desired, still present → drop it.
	desired := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference"}})
	applied := []string{"reference", "timestamp"}
	diff := diffIndexes(desired, map[string]bool{"reference": true, "timestamp": true}, applied)

	assert.Empty(t, diff.toCreate)
	require.Len(t, diff.toDrop, 1)
	assert.Equal(t, "timestamp", diff.toDrop[0].canonical)
	assert.Equal(t, "timestamp", diff.toDrop[0].typeFlag)
}

func TestDiffIndexes_KeepsExternallyCreated(t *testing.T) {
	t.Parallel()

	// "address" exists on the ledger but the operator never created it (not in
	// applied) → it must NOT be dropped even though it isn't desired.
	desired := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference"}})
	diff := diffIndexes(desired, map[string]bool{"reference": true, "address": true}, []string{"reference"})

	assert.Empty(t, diff.toCreate)
	assert.Empty(t, diff.toDrop)
}

func TestDiffIndexes_RelinquishesAlreadyGoneOwnership(t *testing.T) {
	t.Parallel()

	// Applied "timestamp" is no longer desired AND already absent from actual.
	// It must still appear in toDrop so the reconciler relinquishes ownership of
	// it (no drop command will be issued since it is absent) — otherwise a later
	// external recreate would be wrongly treated as operator-owned and dropped.
	desired := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference"}})
	diff := diffIndexes(desired, map[string]bool{"reference": true}, []string{"reference", "timestamp"})

	assert.Empty(t, diff.toCreate)
	require.Len(t, diff.toDrop, 1)
	assert.Equal(t, "timestamp", diff.toDrop[0].canonical)
}

func TestDiffIndexes_EmptyManagedDropsPreviouslyApplied(t *testing.T) {
	t.Parallel()

	// spec.indexes: {} (managed, zero desired) drops only what the operator
	// previously applied and still exists — not externally-created ones.
	desired := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{})
	diff := diffIndexes(desired, map[string]bool{"reference": true, "address": true}, []string{"reference"})

	assert.Empty(t, diff.toCreate)
	require.Len(t, diff.toDrop, 1)
	assert.Equal(t, "reference", diff.toDrop[0].canonical)
}

// ---------------------------------------------------------------------------
// nextAppliedIndexes — ownership tracking (never adopt externally-created)
// ---------------------------------------------------------------------------

func TestNextAppliedIndexes_TracksCreatesAndDrops(t *testing.T) {
	t.Parallel()

	diff := indexDiff{
		toCreate: []managedIndex{{canonical: "timestamp"}},
		toDrop:   []managedIndex{{canonical: "address"}},
	}
	got := nextAppliedIndexes([]string{"reference", "address"}, diff)

	assert.Equal(t, []string{"reference", "timestamp"}, got)
}

// TestNextAppliedIndexes_DoesNotAdoptExisting is the ownership case from the
// review: a desired index that already existed (so it is NOT in toCreate) must
// not be recorded as owned, otherwise a later removal from spec would drop an
// index the operator never created.
func TestNextAppliedIndexes_DoesNotAdoptExisting(t *testing.T) {
	t.Parallel()

	// desired={reference}, actual already has reference => toCreate empty.
	desired := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference"}})
	diff := diffIndexes(desired, map[string]bool{"reference": true}, nil)
	require.Empty(t, diff.toCreate)
	require.Empty(t, diff.toDrop)

	applied := nextAppliedIndexes(nil, diff)
	assert.Empty(t, applied, "pre-existing index must not be adopted as owned")

	// Now the user removes it from spec: it must NOT be dropped, since the
	// operator never owned it.
	emptyDesired := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{})
	drop := diffIndexes(emptyDesired, map[string]bool{"reference": true}, applied)
	assert.Empty(t, drop.toDrop, "externally-created index must never be dropped")
}

func TestNextAppliedIndexes_RecreatedIndexStaysOwned(t *testing.T) {
	t.Parallel()

	// Operator-owned "reference" was manually dropped out-of-band (actual empty)
	// but is still desired => it is recreated and remains owned.
	desired := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference"}})
	diff := diffIndexes(desired, map[string]bool{}, []string{"reference"})
	require.Len(t, diff.toCreate, 1)

	assert.Equal(t, []string{"reference"}, nextAppliedIndexes([]string{"reference"}, diff))
}

func TestNextAppliedIndexes_Empty(t *testing.T) {
	t.Parallel()

	assert.Nil(t, nextAppliedIndexes(nil, indexDiff{}))
}

// TestNextAppliedIndexes_PartialProgress models the reconciler recording only
// the operations that succeeded (createdOK/droppedOK) before an error return:
// index "a" was created but "b" failed, so ownership must include "a".
func TestNextAppliedIndexes_PartialProgress(t *testing.T) {
	t.Parallel()

	got := nextAppliedIndexes(nil, indexDiff{toCreate: []managedIndex{{canonical: "a"}}})
	assert.Equal(t, []string{"a"}, got)
}

func TestDiffIndexes_MixedWithMetadataDrop(t *testing.T) {
	t.Parallel()

	desired := desiredIndexes(&ledgerv1alpha1.LedgerIndexesSpec{
		Transaction: []string{"reference"},
		Metadata:    []ledgerv1alpha1.MetadataIndexSpec{{Target: "account", Key: "category", Type: "string"}},
	})
	applied := []string{"timestamp", "metadata:account:old"}
	actual := map[string]bool{"timestamp": true, "metadata:account:old": true}

	diff := diffIndexes(desired, actual, applied)

	assert.ElementsMatch(t, []string{"reference", "metadata:account:category"}, canonicalsOf(diff.toCreate))
	assert.ElementsMatch(t, []string{"timestamp", "metadata:account:old"}, canonicalsOf(diff.toDrop))

	// The metadata drop must carry reconstructed target/key for the CLI call.
	for _, mi := range diff.toDrop {
		if mi.canonical == "metadata:account:old" {
			assert.Equal(t, metadataTypeFlag, mi.typeFlag)
			assert.Equal(t, "account", mi.target)
			assert.Equal(t, "old", mi.key)
		}
	}
}
