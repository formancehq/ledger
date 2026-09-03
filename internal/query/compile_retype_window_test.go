package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Field conditions are validated and encoded under the type BOUND to the
// served index version, never the live schema. During a retype's conversion
// window the schema already says the new type while the served version still
// carries the old one — compiling under the schema would scan the new type's
// tagged byte ranges over old-encoded rows and return partial results
// (EN-1724). The old kind must stay valid over the complete old index for the
// whole window, and the new kind must be rejected until the atomic switch —
// exactly as if the retype had not happened yet.
func TestCompile_FieldConditionBindsToTheVersionType(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier")
	indexRegistry := staticIndexLookup{
		indexes.KeyFor(ledgerName, id): {Ledger: ledgerName, Id: id},
	}
	info := &commonpb.LedgerInfo{Name: ledgerName}

	// Mid-window: the schema has flipped to INT64, the served version is
	// still the STRING-encoded one.
	schema := map[string]*commonpb.MetadataFieldSchema{
		"tier": {Type: commonpb.MetadataType_METADATA_TYPE_INT64, Revision: 2},
	}
	boundToString := func(string) (readstore.ResolvedIndexVersion, bool, error) {
		return readstore.ResolvedIndexVersion{
			Version:      1,
			Type:         commonpb.MetadataType_METADATA_TYPE_STRING,
			TypeDeclared: true,
			Revision:     1,
			BindingKnown: true,
		}, true, nil
	}

	stringCond := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{
		Field: &commonpb.FieldCondition{
			Field: &commonpb.FieldRef{Metadata: "tier"},
			Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{
				Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "gold"},
			}},
		},
	}}
	intCond := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{
		Field: &commonpb.FieldCondition{
			Field:     &commonpb.FieldRef{Metadata: "tier"},
			Condition: &commonpb.FieldCondition_IntCond{IntCond: &commonpb.IntCondition{}},
		},
	}}

	t.Run("old-kind condition stays valid during the window", func(t *testing.T) {
		t.Parallel()

		// The success path builds its scan, so it needs a real (empty)
		// readstore to iterate.
		rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
		require.NoError(t, err)

		t.Cleanup(func() { _ = rs.Close() })

		iter, err := Compile(
			rs.DB(), dal.NewKeyBuilder(), stringCond,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
			nil, schema, info, indexRegistry, boundToString, nil, nil, 0)
		require.NoError(t, err,
			"a string condition over the still-string-encoded version must compile — rejecting it makes the whole window unqueryable")

		iter.Close()
	})

	t.Run("new-kind condition is rejected until the switch", func(t *testing.T) {
		t.Parallel()

		_, err := Compile(
			nil, nil, intCond,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
			nil, schema, info, indexRegistry, boundToString, nil, nil, 0)
		require.Error(t, err,
			"an int condition compiled against string-encoded rows scans a disjoint byte range — partial results, must be refused")

		var compileErr *domain.ErrFilterCompilation
		require.ErrorAs(t, err, &compileErr)
	})

	t.Run("a version bound to no declared type keeps pre-declaration semantics", func(t *testing.T) {
		t.Parallel()

		boundToNothing := func(string) (readstore.ResolvedIndexVersion, bool, error) {
			return readstore.ResolvedIndexVersion{Version: 1, BindingKnown: true}, true, nil
		}

		// The undeclared binding is legal only as the FIRST declaration's
		// direct predecessor.
		firstDeclaration := map[string]*commonpb.MetadataFieldSchema{
			"tier": {Type: commonpb.MetadataType_METADATA_TYPE_INT64, Revision: 1},
		}

		_, err := Compile(
			nil, nil, stringCond,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
			nil, firstDeclaration, info, indexRegistry, boundToNothing, nil, nil, 0)
		require.Error(t, err)

		var notFound *domain.ErrIndexNotFound
		require.ErrorAs(t, err, &notFound,
			"the served version predates any declaration: Field conditions were rejected then, and the window keeps that until the declared-type keyspace is promoted")
	})
}

// A binding more than one schema revision behind is not a live retype window —
// it is a rewound read store re-walking the retype chain (the read index is a
// WAL-less Pebble store; a hard kill rewinds it to the last flush). Serving it
// would time-travel query semantics at a pin far past the retypes it has not
// re-applied, so the gate refuses it as the rebuild in progress it is:
// INDEX_BUILDING, the retryable class the adapter forwards to the leader.
func TestCompile_StaleBindingReadsAsBuilding(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier")
	indexRegistry := staticIndexLookup{
		indexes.KeyFor(ledgerName, id): {Ledger: ledgerName, Id: id},
	}
	info := &commonpb.LedgerInfo{Name: ledgerName}

	// Third declaration of tier: INT32 → STRING → INT64.
	schema := map[string]*commonpb.MetadataFieldSchema{
		"tier": {Type: commonpb.MetadataType_METADATA_TYPE_INT64, Revision: 3},
	}

	intCond := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{
		Field: &commonpb.FieldCondition{
			Field:     &commonpb.FieldRef{Metadata: "tier"},
			Condition: &commonpb.FieldCondition_IntCond{IntCond: &commonpb.IntCondition{}},
		},
	}}

	resolver := func(rev uint32, typ commonpb.MetadataType, declared bool) readstore.IndexVersionResolver {
		return func(string) (readstore.ResolvedIndexVersion, bool, error) {
			return readstore.ResolvedIndexVersion{
				Version:      1,
				Type:         typ,
				TypeDeclared: declared,
				Revision:     rev,
				BindingKnown: true,
			}, true, nil
		}
	}

	requireBuilding := func(t *testing.T, err error) {
		t.Helper()
		require.Error(t, err)

		var building *domain.ErrIndexBuilding
		require.ErrorAs(t, err, &building)
	}

	t.Run("two revisions behind is a rebuild, not a window", func(t *testing.T) {
		t.Parallel()

		_, err := Compile(
			nil, nil, intCond,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
			nil, schema, info, indexRegistry,
			resolver(1, commonpb.MetadataType_METADATA_TYPE_INT32, true), nil, nil, 0)
		requireBuilding(t, err)
	})

	t.Run("an undeclared binding behind a re-declared schema is a rebuild", func(t *testing.T) {
		t.Parallel()

		_, err := Compile(
			nil, nil, intCond,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
			nil, schema, info, indexRegistry,
			resolver(0, 0, false), nil, nil, 0)
		requireBuilding(t, err)
	})

	t.Run("the direct predecessor stays the served window", func(t *testing.T) {
		t.Parallel()

		// Revision 2 = STRING; a string condition over it must still compile.
		stringCond := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: "tier"},
				Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{
					Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "gold"},
				}},
			},
		}}

		rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
		require.NoError(t, err)

		t.Cleanup(func() { _ = rs.Close() })

		iter, err := Compile(
			rs.DB(), dal.NewKeyBuilder(), stringCond,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
			nil, schema, info, indexRegistry,
			resolver(2, commonpb.MetadataType_METADATA_TYPE_STRING, true), nil, nil, 0)
		require.NoError(t, err)

		iter.Close()
	})

	t.Run("a converged binding serves", func(t *testing.T) {
		t.Parallel()

		rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
		require.NoError(t, err)

		t.Cleanup(func() { _ = rs.Close() })

		iter, err := Compile(
			rs.DB(), dal.NewKeyBuilder(), intCond,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
			nil, schema, info, indexRegistry,
			resolver(3, commonpb.MetadataType_METADATA_TYPE_INT64, true), nil, nil, 0)
		require.NoError(t, err)

		iter.Close()
	})
}
