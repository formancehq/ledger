package query_test

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// txid returns the 8-byte big-endian transaction ID encoding.
func txid(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)

	return b
}

func addEvent(t *testing.T, s *readstore.Store, kb *dal.KeyBuilder, ledger, metaKey string, encodedValue []byte, entity []byte, seq uint64, op byte) {
	t.Helper()

	key := readstore.MetadataIndexEventKeyV(kb, ledger, readstore.NamespaceTransaction, metaKey, 1, encodedValue, entity, seq, op)
	require.NoError(t, s.DB().Set(key, nil, pebble.NoSync))
}

func drainForward(it readstore.EntityIterator) []uint64 {
	defer it.Close()

	var out []uint64
	for it.Next() {
		out = append(out, binary.BigEndian.Uint64(it.Current()))
	}

	if err := it.Err(); err != nil {
		panic(err)
	}

	return out
}

func drainReverse(it query.ReverseEntityIterator) []uint64 {
	defer it.Close()

	var out []uint64
	for it.Next() {
		out = append(out, binary.BigEndian.Uint64(it.Current()))
	}

	if err := it.Err(); err != nil {
		panic(err)
	}

	return out
}

// TestCompileReverse_MatchesReversedForward pins the descending-vs-ascending
// parity for both streaming entity-ordered leaves and the materializing
// value-ordered fallback, across boolean compositions. It is the unit-level
// mirror of the full-traversal acceptance criterion.
func TestCompileReverse_MatchesReversedForward(t *testing.T) {
	t.Parallel()

	const ledger = "l"

	rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	kb := dal.NewKeyBuilder()

	stringEvent := func(metaKey, value string, id, seq uint64, op byte) {
		addEvent(t, rs, kb, ledger, metaKey, readstore.EncodeString(nil, value), txid(id), seq, op)
	}
	uintEvent := func(metaKey string, value, id, seq uint64, op byte) {
		addEvent(t, rs, kb, ledger, metaKey, readstore.EncodeUint64(nil, value), txid(id), seq, op)
	}

	// tier (string), cat (string), score (uint64) across six transactions.
	stringEvent("tier", "gold", 1, 1, readstore.MetadataEventAdd)
	stringEvent("tier", "silver", 2, 1, readstore.MetadataEventAdd)
	stringEvent("tier", "gold", 5, 1, readstore.MetadataEventAdd)
	stringEvent("tier", "gold", 7, 1, readstore.MetadataEventAdd)

	stringEvent("cat", "a", 1, 1, readstore.MetadataEventAdd)
	stringEvent("cat", "a", 2, 1, readstore.MetadataEventAdd)
	stringEvent("cat", "b", 5, 1, readstore.MetadataEventAdd)
	stringEvent("cat", "a", 7, 1, readstore.MetadataEventAdd)

	uintEvent("score", 10, 1, 1, readstore.MetadataEventAdd)
	uintEvent("score", 20, 2, 1, readstore.MetadataEventAdd)
	uintEvent("score", 10, 5, 1, readstore.MetadataEventAdd)
	uintEvent("score", 30, 7, 1, readstore.MetadataEventAdd)

	schema := map[string]*commonpb.MetadataFieldSchema{
		"tier":  {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		"cat":   {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		"score": {Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
	}
	info := &commonpb.LedgerInfo{Name: ledger}

	byType := map[string]commonpb.MetadataType{
		"tier":  commonpb.MetadataType_METADATA_TYPE_STRING,
		"cat":   commonpb.MetadataType_METADATA_TYPE_STRING,
		"score": commonpb.MetadataType_METADATA_TYPE_UINT64,
	}

	registry := staticIndexLookup{}
	for _, metaKey := range []string{"tier", "cat", "score"} {
		id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, metaKey)
		registry[indexes.KeyFor(ledger, id)] = &commonpb.Index{Ledger: ledger, Id: id}
	}

	res := func(canonical string) (readstore.ResolvedIndexVersion, bool, error) {
		var metaKey string
		switch canonical {
		case indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "tier")):
			metaKey = "tier"
		case indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "cat")):
			metaKey = "cat"
		case indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "score")):
			metaKey = "score"
		default:
			return readstore.ResolvedIndexVersion{}, false, nil
		}

		return readstore.ResolvedIndexVersion{
			Version:      1,
			Type:         byType[metaKey],
			TypeDeclared: true,
			BindingKnown: true,
		}, true, nil
	}

	stringEq := func(field, value string) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
			Field: &commonpb.FieldRef{Metadata: field},
			Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{
				Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value},
			}},
		}}}
	}
	uintRange := func(field string, lo, hi uint64) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
			Field:     &commonpb.FieldRef{Metadata: field},
			Condition: &commonpb.FieldCondition_UintCond{UintCond: &commonpb.UintCondition{Min: &lo, Max: &hi}},
		}}}
	}
	and := func(filters ...*commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: filters}}}
	}
	or := func(filters ...*commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: filters}}}
	}

	cases := []struct {
		name   string
		filter *commonpb.QueryFilter
	}{
		{"string equality leaf", stringEq("tier", "gold")},
		{"or of equality leaves", or(stringEq("tier", "gold"), stringEq("tier", "silver"))},
		{"and of equality leaves", and(stringEq("tier", "gold"), stringEq("cat", "a"))},
		{"value-ordered uint range fallback", uintRange("score", 15, 40)},
		{"composition containing a fallback", and(stringEq("tier", "gold"), uintRange("score", 15, 40))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fwd, err := query.Compile(rs.DB(), dal.NewKeyBuilder(), tc.filter,
				commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, ledger,
				nil, schema, info, registry, res, nil, nil, 0)
			require.NoError(t, err)

			ascending := drainForward(fwd)

			rev, err := query.CompileReverse(rs.DB(), dal.NewKeyBuilder(), tc.filter,
				commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, ledger,
				nil, schema, info, registry, res, nil, nil, 0)
			require.NoError(t, err)

			descending := drainReverse(rev)

			want := slices.Clone(ascending)
			slices.Reverse(want)

			require.Equal(t, want, descending,
				"descending traversal must equal the reversed reference set")
		})
	}
}
