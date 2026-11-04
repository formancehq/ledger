package query_test

import (
	"encoding/binary"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// These expectations name fixture identities independently of either compiler.
// Both public entry points must preserve literal/parameter selection and role
// semantics when their direction-independent preparation is shared.
func TestCompileLeafResolution_AddressSelection(t *testing.T) {
	t.Parallel()
	store := parityStore(t)
	batch := store.NewBatch()
	for _, id := range []uint64{102, 105, 108} {
		account := fmt.Sprintf("accounts:%02d", id-100)
		require.NoError(t, batch.SetBytes(readstore.AccountTxKey(dal.NewKeyBuilder(), readstore.PrefixDestinationAccountTx, parityLedger, account, id), nil))
	}
	require.NoError(t, batch.Commit())

	for _, parameterized := range []bool{false, true} {
		t.Run(fmt.Sprintf("destination exact positive parameterized=%t", parameterized), func(t *testing.T) {
			t.Parallel()
			am := &commonpb.AddressMatch{Role: commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "accounts:02"}}
			if parameterized {
				am.Match = &commonpb.AddressMatch_ParamExact{ParamExact: "address"}
			}
			options := leafResolutionOptions(store, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
			options.Parameters = map[string]*commonpb.ParameterValue{"address": {Value: &commonpb.ParameterValue_StringValue{StringValue: "accounts:02"}}}
			assertLeafResolutionEntities(t, &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: am}}, options, []string{"102"})
		})
	}

	for _, tc := range []struct {
		name          string
		target        commonpb.QueryTarget
		role          commonpb.AddressRole
		prefix, exact []string
	}{
		{"accounts", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, commonpb.AddressRole_ADDRESS_ROLE_ANY,
			[]string{"accounts:00", "accounts:01", "accounts:02", "accounts:03", "accounts:04", "accounts:05", "accounts:06", "accounts:07", "accounts:08", "accounts:09"}, []string{"accounts:03"}},
		{"transactions any", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, commonpb.AddressRole_ADDRESS_ROLE_ANY,
			[]string{"100", "101", "102", "103", "104", "105", "106", "107", "108", "109"}, []string{"103"}},
		{"transactions source", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, commonpb.AddressRole_ADDRESS_ROLE_SOURCE,
			[]string{"100", "103", "106", "109"}, []string{"103"}},
		{"transactions destination", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION,
			[]string{"102", "105", "108"}, []string{}},
	} {
		for _, shape := range []string{"literal prefix", "parameter prefix", "literal exact", "parameter exact", "absent exact"} {
			t.Run(tc.name+"/"+shape, func(t *testing.T) {
				t.Parallel()
				am := &commonpb.AddressMatch{Role: tc.role}
				want := tc.prefix
				value := "accounts:0"
				switch shape {
				case "literal prefix":
					am.Match = &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: value}
				case "parameter prefix":
					am.Match = &commonpb.AddressMatch_ParamPrefix{ParamPrefix: "address"}
				case "literal exact":
					value = "accounts:03"
					want = tc.exact
					am.Match = &commonpb.AddressMatch_HardcodedExact{HardcodedExact: value}
				case "parameter exact":
					value = "accounts:03"
					want = tc.exact
					am.Match = &commonpb.AddressMatch_ParamExact{ParamExact: "address"}
				case "absent exact":
					want = []string{}
					am.Match = &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "absent"}
				}
				options := leafResolutionOptions(store, tc.target)
				options.Parameters = map[string]*commonpb.ParameterValue{"address": {Value: &commonpb.ParameterValue_StringValue{StringValue: value}}}
				filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: am}}
				assertLeafResolutionEntities(t, filter, options, want)
			})
		}
	}
}

func TestCompileLeafResolution_ReferenceAndAssetSelection(t *testing.T) {
	t.Parallel()
	store := parityStore(t)
	for _, tc := range []struct {
		name   string
		target commonpb.QueryTarget
		filter *commonpb.QueryFilter
		want   []string
	}{
		{"literal reference", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			&commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{Reference: &commonpb.ReferenceCondition{Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "ref-a"}}}}},
			[]string{"100", "103", "106", "109"}},
		{"parameter reference", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			&commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{Reference: &commonpb.ReferenceCondition{Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Param{Param: "reference"}}}}},
			[]string{"100", "103", "106", "109"}},
		{"asset at pin", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, hasAssetFilter("USD", 2),
			[]string{"accounts:00", "accounts:02", "accounts:04", "accounts:06", "accounts:08", "accounts:10"}},
		{"asset precision distinguishes cells", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, hasAssetFilter("USD", 3), []string{}},
		{"asset base distinguishes cells", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, hasAssetFilter("EUR", 2), []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			options := leafResolutionOptions(store, tc.target)
			options.Parameters = map[string]*commonpb.ParameterValue{"reference": {Value: &commonpb.ParameterValue_StringValue{StringValue: "ref-a"}}}
			assertLeafResolutionEntities(t, tc.filter, options, tc.want)
		})
	}
}

func TestCompileLeafResolution_ParameterErrorsAndReadinessPrecedence(t *testing.T) {
	t.Parallel()
	store := parityStore(t)
	for _, leaf := range []string{"address prefix", "address exact", "reference"} {
		for _, failure := range []string{"missing", "nil", "wrong type", "undeclared", "building"} {
			t.Run(leaf+"/"+failure, func(t *testing.T) {
				t.Parallel()
				options := leafResolutionOptions(store, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
				var filter *commonpb.QueryFilter
				switch leaf {
				case "address prefix":
					filter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{Match: &commonpb.AddressMatch_ParamPrefix{ParamPrefix: "value"}}}}
				case "address exact":
					filter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{Match: &commonpb.AddressMatch_ParamExact{ParamExact: "value"}}}}
				case "reference":
					filter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{Reference: &commonpb.ReferenceCondition{Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Param{Param: "value"}}}}}
				}
				want := `parameter "value" not provided`
				switch failure {
				case "nil":
					options.Parameters = map[string]*commonpb.ParameterValue{"value": nil}
					want = `parameter "value" has a nil value, expected string`
				case "wrong type":
					options.Parameters = map[string]*commonpb.ParameterValue{"value": {Value: &commonpb.ParameterValue_BoolValue{BoolValue: true}}}
					want = `expected string value, got bool`
				case "undeclared":
					options.IndexRegistry = staticIndexLookup{}
				case "building":
					options.IndexVersionFor = func(string) (readstore.ResolvedIndexVersion, bool, error) {
						return readstore.ResolvedIndexVersion{}, true, nil
					}
				}
				// Parameters remain absent for readiness failures: readiness must win.
				var previous string
				for _, reverse := range []bool{false, true} {
					var err error
					if reverse {
						_, err = query.CompileReverse(filter, options)
					} else {
						_, err = query.Compile(filter, options)
					}
					require.Error(t, err)
					switch failure {
					case "undeclared":
						var cause *domain.ErrIndexNotFound
						require.ErrorAs(t, err, &cause)
					case "building":
						var cause *domain.ErrIndexBuilding
						require.ErrorAs(t, err, &cause)
					default:
						require.Contains(t, err.Error(), want)
					}
					if reverse {
						require.Equal(t, previous, err.Error())
					}
					previous = err.Error()
				}
			})
		}
	}
}

func TestCompileLeafResolution_EmptyStringsAndUnindexedAccounts(t *testing.T) {
	t.Parallel()
	store := parityStore(t)
	for _, target := range []commonpb.QueryTarget{commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS} {
		for _, shape := range []string{"literal prefix", "parameter prefix", "literal exact", "parameter exact"} {
			t.Run(target.String()+"/"+shape, func(t *testing.T) {
				t.Parallel()
				options := leafResolutionOptions(store, target)
				// Account address selection is main-store-only and needs no declaration.
				if target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
					options.IndexRegistry = staticIndexLookup{}
				}
				options.Parameters = map[string]*commonpb.ParameterValue{"address": {Value: &commonpb.ParameterValue_StringValue{StringValue: ""}}}
				am := &commonpb.AddressMatch{}
				want := []string{}
				switch shape {
				case "literal prefix":
					am.Match = &commonpb.AddressMatch_HardcodedPrefix{}
				case "parameter prefix":
					am.Match = &commonpb.AddressMatch_ParamPrefix{ParamPrefix: "address"}
				case "literal exact":
					am.Match = &commonpb.AddressMatch_HardcodedExact{}
				case "parameter exact":
					am.Match = &commonpb.AddressMatch_ParamExact{ParamExact: "address"}
				}
				if shape == "literal prefix" || shape == "parameter prefix" {
					if target == commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
						want = []string{"accounts:00", "accounts:01", "accounts:02", "accounts:03", "accounts:04", "accounts:05", "accounts:06", "accounts:07", "accounts:08", "accounts:09", "accounts:10", "accounts:11"}
					} else {
						want = []string{"100", "101", "102", "103", "104", "105", "106", "107", "108", "109", "110", "111"}
					}
				}
				assertLeafResolutionEntities(t, &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: am}}, options, want)
			})
		}
	}
	for _, parameterized := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty reference parameterized=%t", parameterized), func(t *testing.T) {
			t.Parallel()
			options := leafResolutionOptions(store, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
			options.Parameters = map[string]*commonpb.ParameterValue{"reference": {Value: &commonpb.ParameterValue_StringValue{StringValue: ""}}}
			cond := &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{}}
			if parameterized {
				cond.Value = &commonpb.StringCondition_Param{Param: "reference"}
			}
			filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{Reference: &commonpb.ReferenceCondition{Cond: cond}}}
			assertLeafResolutionEntities(t, filter, options, []string{})
		})
	}
}

func TestCompileLeafResolution_AddressRoleReadiness(t *testing.T) {
	t.Parallel()
	store := parityStore(t)
	for _, tc := range []struct {
		role  commonpb.AddressRole
		field commonpb.TransactionBuiltinIndex
	}{
		{commonpb.AddressRole_ADDRESS_ROLE_ANY, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS},
		{commonpb.AddressRole_ADDRESS_ROLE_SOURCE, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS},
		{commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS},
	} {
		t.Run(tc.role.String(), func(t *testing.T) {
			t.Parallel()
			options := leafResolutionOptions(store, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
			registry := parityRegistry()
			delete(registry, indexes.KeyFor(parityLedger, indexes.TxBuiltinID(tc.field)))
			options.IndexRegistry = registry
			// Other role indexes remain ready. An absent parameter distinguishes the
			// role-specific readiness gate from the later value resolution.
			filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{Role: tc.role, Match: &commonpb.AddressMatch_ParamExact{ParamExact: "address"}}}}
			for _, reverse := range []bool{false, true} {
				var err error
				if reverse {
					_, err = query.CompileReverse(filter, options)
				} else {
					_, err = query.Compile(filter, options)
				}
				var cause *domain.ErrIndexNotFound
				require.ErrorAs(t, err, &cause)
			}
		})
	}
}

func leafResolutionOptions(store *readstore.Store, target commonpb.QueryTarget) query.CompileOptions {
	return query.CompileOptions{IndexReader: store.DB(), MainReader: store.DB(), KeyBuilder: dal.NewKeyBuilder(), Target: target, LedgerName: parityLedger, LedgerInfo: parityInfo(), IndexRegistry: parityRegistry(), IndexVersionFor: parityResolver(), Pin: parityPin}
}

func assertLeafResolutionEntities(t *testing.T, filter *commonpb.QueryFilter, options query.CompileOptions, want []string) {
	t.Helper()
	for _, reverse := range []bool{false, true} {
		var iter interface {
			Next() bool
			Current() []byte
			Err() error
			Close()
		}
		var err error
		if reverse {
			iter, err = query.CompileReverse(filter, options)
		} else {
			iter, err = query.Compile(filter, options)
		}
		require.NoError(t, err)
		got := []string{}
		for iter.Next() {
			entity := string(iter.Current())
			if options.Target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS || options.Target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
				entity = strconv.FormatUint(binary.BigEndian.Uint64(iter.Current()), 10)
			}
			got = append(got, entity)
		}
		require.NoError(t, iter.Err())
		iter.Close()
		expected := slices.Clone(want)
		if reverse {
			slices.Reverse(expected)
		}
		require.Equal(t, expected, got, "reverse=%t", reverse)
	}
}
