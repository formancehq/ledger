package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
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

	id := indexes.MetadataID(ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, "tier")
	indexRegistry := staticIndexLookup{
		indexes.KeyFor(ledgerName, id): {Ledger: ledgerName, Id: id},
	}
	info := &ledgerpb.LedgerInfo{Name: ledgerName}

	// Mid-window: the schema has flipped to INT64, the served version is
	// still the STRING-encoded one.
	schema := map[string]*ledgerpb.MetadataFieldSchema{
		"tier": {Type: ledgerpb.MetadataType_METADATA_TYPE_INT64},
	}
	boundToString := func(string) (readstore.ResolvedIndexVersion, bool, error) {
		return readstore.ResolvedIndexVersion{
			Version:      1,
			Type:         ledgerpb.MetadataType_METADATA_TYPE_STRING,
			TypeDeclared: true,
			BindingKnown: true,
		}, true, nil
	}

	stringCond := &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_Field{
		Field: &ledgerpb.FieldCondition{
			Field: &ledgerpb.FieldRef{Metadata: "tier"},
			Condition: &ledgerpb.FieldCondition_StringCond{StringCond: &ledgerpb.StringCondition{
				Value: &ledgerpb.StringCondition_Hardcoded{Hardcoded: "gold"},
			}},
		},
	}}
	intCond := &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_Field{
		Field: &ledgerpb.FieldCondition{
			Field:     &ledgerpb.FieldRef{Metadata: "tier"},
			Condition: &ledgerpb.FieldCondition_IntCond{IntCond: &ledgerpb.IntCondition{}},
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
			ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
			nil, schema, info, indexRegistry, boundToString, nil, nil, 0)
		require.NoError(t, err,
			"a string condition over the still-string-encoded version must compile — rejecting it makes the whole window unqueryable")

		iter.Close()
	})

	t.Run("new-kind condition is rejected until the switch", func(t *testing.T) {
		t.Parallel()

		_, err := Compile(
			nil, nil, intCond,
			ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
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

		_, err := Compile(
			nil, nil, stringCond,
			ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
			nil, schema, info, indexRegistry, boundToNothing, nil, nil, 0)
		require.Error(t, err)

		var notFound *domain.ErrIndexNotFound
		require.ErrorAs(t, err, &notFound,
			"the served version predates any declaration: Field conditions were rejected then, and the window keeps that until the declared-type keyspace is promoted")
	})
}
