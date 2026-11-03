package query

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// A negative bound on an unsigned metadata field is refused whether or not an
// AND merge would make it redundant (EN-3239): grouping and parameterization
// must not change whether a query is accepted.
func TestCompile_NegativeBoundOnUnsignedFieldIsRejectedBeforeMerge(t *testing.T) {
	t.Parallel()

	const (
		ledgerName = "ledger1"
		pin        = 1000
	)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "n")
	registry := staticIndexLookup{indexes.KeyFor(ledgerName, id): {Ledger: ledgerName, Id: id}}
	schema := map[string]*commonpb.MetadataFieldSchema{
		"n": {Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
	}
	servedAsUint64 := func(string) (readstore.ResolvedIndexVersion, bool, error) {
		return readstore.ResolvedIndexVersion{
			Version:      1,
			Type:         commonpb.MetadataType_METADATA_TYPE_UINT64,
			TypeDeclared: true,
			BindingKnown: true,
		}, true, nil
	}

	store, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	// accounts:c carries no `n`, so it must never match a range on `n`.
	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	for i, account := range []string{"accounts:a", "accounts:b"} {
		require.NoError(t, batch.SetBytes(readstore.MetadataIndexEventKeyV(
			kb, ledgerName, readstore.NamespaceAccount, "n", 1,
			readstore.EncodeUint64(nil, uint64(i)), []byte(account),
			uint64(10+i), readstore.MetadataEventAdd), nil))
	}
	require.NoError(t, batch.Commit())

	options := func(params map[string]*commonpb.ParameterValue) CompileOptions {
		return CompileOptions{
			IndexReader:     store.DB(),
			MainReader:      store.DB(),
			KeyBuilder:      dal.NewKeyBuilder(),
			Target:          commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			LedgerName:      ledgerName,
			Parameters:      params,
			Schema:          schema,
			LedgerInfo:      &commonpb.LedgerInfo{Name: ledgerName},
			IndexRegistry:   registry,
			IndexVersionFor: servedAsUint64,
			Pin:             pin,
		}
	}

	intLeaf := func(cond *commonpb.IntCondition) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
			Field:     &commonpb.FieldRef{Metadata: "n"},
			Condition: &commonpb.FieldCondition_IntCond{IntCond: cond},
		}}}
	}
	gte := func(v int64) *commonpb.QueryFilter { return intLeaf(&commonpb.IntCondition{Min: &v}) }
	lte := func(v int64) *commonpb.QueryFilter { return intLeaf(&commonpb.IntCondition{Max: &v}) }
	gteParam := intLeaf(&commonpb.IntCondition{ParamMin: "lo"})
	and := func(fs ...*commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: fs}}}
	}

	drainAscending := func(t *testing.T, filter *commonpb.QueryFilter, params map[string]*commonpb.ParameterValue) ([]string, error) {
		t.Helper()

		iter, err := Compile(filter, options(params))
		if err != nil {
			return nil, err
		}
		defer iter.Close()

		var out []string
		for iter.Next() {
			out = append(out, string(iter.Current()))
		}

		return out, iter.Err()
	}
	drainDescending := func(t *testing.T, filter *commonpb.QueryFilter, params map[string]*commonpb.ParameterValue) ([]string, error) {
		t.Helper()

		iter, err := CompileReverse(filter, options(params))
		if err != nil {
			return nil, err
		}
		defer iter.Close()

		items, _, err := readstore.PaginateReverse(iter, 100, nil)
		if err != nil {
			return nil, err
		}

		out := make([]string, 0, len(items))
		for _, item := range items {
			out = append(out, string(item))
		}
		slices.Reverse(out)

		return out, nil
	}
	compilers := []struct {
		name  string
		drain func(*testing.T, *commonpb.QueryFilter, map[string]*commonpb.ParameterValue) ([]string, error)
	}{
		{"ascending", drainAscending},
		{"descending", drainDescending},
	}

	rejected := []struct {
		name    string
		filter  *commonpb.QueryFilter
		params  map[string]*commonpb.ParameterValue
		message string
	}{
		{name: "flat", filter: and(gte(-1), gte(0)), message: "negative min bound -1"},
		{name: "flat reversed", filter: and(gte(0), gte(-1)), message: "negative min bound -1"},
		{name: "flat with upper bound", filter: and(gte(-1), gte(0), lte(1)), message: "negative min bound -1"},
		{name: "nested", filter: and(and(gte(-1)), gte(0)), message: "negative min bound -1"},
		{
			name:    "int64 parameter",
			filter:  and(gteParam, gte(0)),
			params:  map[string]*commonpb.ParameterValue{"lo": {Value: &commonpb.ParameterValue_Int64Value{Int64Value: -1}}},
			message: "negative int64 value -1",
		},
		{
			name:    "string parameter",
			filter:  and(gteParam, gte(0)),
			params:  map[string]*commonpb.ParameterValue{"lo": {Value: &commonpb.ParameterValue_StringValue{StringValue: "-1"}}},
			message: `cannot parse "-1" as uint64`,
		},
	}

	for _, c := range compilers {
		for _, tc := range rejected {
			t.Run(c.name+"/rejects "+tc.name, func(t *testing.T) {
				t.Parallel()

				_, err := c.drain(t, tc.filter, tc.params)
				require.Error(t, err)

				var compileErr *domain.ErrFilterCompilation
				require.ErrorAs(t, err, &compileErr)
				require.Contains(t, compileErr.Detail, tc.message)
			})
		}
	}

	accepted := []struct {
		name   string
		filter *commonpb.QueryFilter
		want   []string
	}{
		{name: "non-negative lower bounds", filter: and(gte(0), gte(1)), want: []string{"accounts:b"}},
		{name: "non-negative closed range", filter: and(gte(0), lte(1)), want: []string{"accounts:a", "accounts:b"}},
		{name: "nested non-negative bounds", filter: and(and(gte(0)), gte(1)), want: []string{"accounts:b"}},
	}

	for _, c := range compilers {
		for _, tc := range accepted {
			t.Run(c.name+"/accepts "+tc.name, func(t *testing.T) {
				t.Parallel()

				got, err := c.drain(t, tc.filter, nil)
				require.NoError(t, err)
				require.Equal(t, tc.want, got)
			})
		}
	}
}
