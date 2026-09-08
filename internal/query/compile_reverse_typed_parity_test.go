package query_test

import (
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Typed leaves must preserve both the predicate bounds and entity order. Values
// deliberately do not follow transaction IDs, including values at both extrema.
func TestCompileReverse_TypedMetadataParity(t *testing.T) {
	t.Parallel()
	const ledger = "typed"
	rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rs.Close()) })
	kb := dal.NewKeyBuilder()
	schema := map[string]*commonpb.MetadataFieldSchema{
		"signed":   {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
		"unsigned": {Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
		"date":     {Type: commonpb.MetadataType_METADATA_TYPE_DATETIME},
		"active":   {Type: commonpb.MetadataType_METADATA_TYPE_BOOL},
	}
	registry := staticIndexLookup{}
	versions := map[string]readstore.ResolvedIndexVersion{}
	for key, field := range schema {
		id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key)
		registry[indexes.KeyFor(ledger, id)] = &commonpb.Index{Ledger: ledger, Id: id}
		versions[indexes.Canonical(id)] = readstore.ResolvedIndexVersion{Version: 1, Type: field.GetType(), TypeDeclared: true, BindingKnown: true}
	}
	resolver := func(canonical string) (readstore.ResolvedIndexVersion, bool, error) {
		version, ok := versions[canonical]

		return version, ok, nil
	}
	for _, row := range []struct {
		id       uint64
		signed   int64
		unsigned uint64
		active   bool
	}{
		{1, 10, 10, true},
		{2, math.MaxInt64, math.MaxUint64, false},
		{3, -10, 0, true},
		{4, 0, 20, false},
		{5, math.MinInt64, 30, true},
		{6, 10, 10, false},
	} {
		for key, value := range map[string][]byte{
			"signed":   readstore.EncodeInt64(nil, row.signed),
			"unsigned": readstore.EncodeUint64(nil, row.unsigned),
			// Datetimes use signed microseconds, including pre-epoch values.
			"date":   readstore.EncodeInt64(nil, row.signed),
			"active": readstore.EncodeBool(nil, row.active),
		} {
			addEvent(t, rs, kb, ledger, key, value, txid(row.id), 1, readstore.MetadataEventAdd)
		}
	}
	cases := []struct {
		name      string
		key       string
		condition any
		want      []uint64
	}{
		{"signed equality", "signed", &commonpb.IntCondition{Min: new(int64(10)), Max: new(int64(10))}, []uint64{1, 6}},
		{"signed inclusive range", "signed", &commonpb.IntCondition{Min: new(int64(-10)), Max: new(int64(10))}, []uint64{1, 3, 4, 6}},
		{"signed exclusive range", "signed", &commonpb.IntCondition{Min: new(int64(-10)), Max: new(int64(10)), MinExclusive: true, MaxExclusive: true}, []uint64{4}},
		{"signed lower unbounded", "signed", &commonpb.IntCondition{Max: new(int64(-10))}, []uint64{3, 5}},
		{"signed upper unbounded", "signed", &commonpb.IntCondition{Min: new(int64(10))}, []uint64{1, 2, 6}},
		{"signed minimum equality", "signed", &commonpb.IntCondition{Min: new(int64(math.MinInt64)), Max: new(int64(math.MinInt64))}, []uint64{5}},
		{"signed maximum equality", "signed", &commonpb.IntCondition{Min: new(int64(math.MaxInt64)), Max: new(int64(math.MaxInt64))}, []uint64{2}},
		{"signed impossible exclusive maximum", "signed", &commonpb.IntCondition{Min: new(int64(math.MaxInt64)), MinExclusive: true}, nil},
		{"signed impossible exclusive minimum", "signed", &commonpb.IntCondition{Max: new(int64(math.MinInt64)), MaxExclusive: true}, nil},
		{"signed inverted bounds", "signed", &commonpb.IntCondition{Min: new(int64(10)), Max: new(int64(-10))}, nil},
		{"unsigned equality", "unsigned", &commonpb.UintCondition{Min: new(uint64(10)), Max: new(uint64(10))}, []uint64{1, 6}},
		{"unsigned inclusive range", "unsigned", &commonpb.UintCondition{Min: new(uint64(10)), Max: new(uint64(30))}, []uint64{1, 4, 5, 6}},
		{"unsigned exclusive range", "unsigned", &commonpb.UintCondition{Min: new(uint64(10)), Max: new(uint64(30)), MinExclusive: true, MaxExclusive: true}, []uint64{4}},
		{"unsigned upper unbounded", "unsigned", &commonpb.UintCondition{Min: new(uint64(30))}, []uint64{2, 5}},
		{"unsigned maximum equality", "unsigned", &commonpb.UintCondition{Min: new(uint64(math.MaxUint64)), Max: new(uint64(math.MaxUint64))}, []uint64{2}},
		{"unsigned impossible exclusive maximum", "unsigned", &commonpb.UintCondition{Min: new(uint64(math.MaxUint64)), MinExclusive: true}, nil},
		{"unsigned impossible exclusive zero", "unsigned", &commonpb.UintCondition{Max: new(uint64(0)), MaxExclusive: true}, nil},
		{"unsigned coerced signed bounds", "unsigned", &commonpb.IntCondition{Min: new(int64(10)), Max: new(int64(30)), MinExclusive: true}, []uint64{4, 5}},
		{"datetime pre-epoch equality", "date", &commonpb.IntCondition{Min: new(int64(-10)), Max: new(int64(-10))}, []uint64{3}},
		{"datetime range across epoch", "date", &commonpb.IntCondition{Min: new(int64(-10)), Max: new(int64(10)), MaxExclusive: true}, []uint64{3, 4}},
		{"bool true", "active", &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: true}}, []uint64{1, 3, 5}},
		{"bool false", "active", &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: false}}, []uint64{2, 4, 6}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fc := &commonpb.FieldCondition{Field: &commonpb.FieldRef{Metadata: tc.key}}
			switch cond := tc.condition.(type) {
			case *commonpb.IntCondition:
				fc.Condition = &commonpb.FieldCondition_IntCond{IntCond: cond}
			case *commonpb.UintCondition:
				fc.Condition = &commonpb.FieldCondition_UintCond{UintCond: cond}
			case *commonpb.BoolCondition:
				fc.Condition = &commonpb.FieldCondition_BoolCond{BoolCond: cond}
			default:
				t.Fatalf("unsupported test condition %T", cond)
			}
			filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: fc}}
			forward, err := query.Compile(rs.DB(), dal.NewKeyBuilder(), filter,
				commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, ledger, nil, schema,
				&commonpb.LedgerInfo{Name: ledger}, registry, resolver, nil, nil, 1)
			require.NoError(t, err)
			ascending := drainForward(forward)
			require.Equal(t, tc.want, ascending)
			reverse, err := query.CompileReverse(rs.DB(), dal.NewKeyBuilder(), filter,
				commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, ledger, nil, schema,
				&commonpb.LedgerInfo{Name: ledger}, registry, resolver, nil, nil, 1)
			require.NoError(t, err)
			wantDescending := slices.Clone(tc.want)
			slices.Reverse(wantDescending)
			require.Equal(t, wantDescending, drainReverse(reverse))
		})
	}
}
