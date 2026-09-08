package query_test

import (
	"bytes"
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

// Exercise the compiler's target-specific branches against explicit sets, so
// forward/reverse parity cannot pass because both directions dropped a row.
func TestCompileReverse_TargetParity(t *testing.T) {
	t.Parallel()
	const ledger = "parity"
	main := newTestStore(t)
	rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rs.Close()) })
	kb := dal.NewKeyBuilder()
	put := func(key, value []byte) { require.NoError(t, rs.DB().Set(key, value, pebble.NoSync)) }
	session := main.OpenWriteSession()
	// IDs and timestamps deliberately have different orders, with a timestamp tie.
	for i, id := range []uint64{1, 2, 5, 7} {
		key := kb.Reset().PutZonePrefix(dal.ZoneAttributes, dal.SubAttrTransaction).PutLedgerNameFixed(ledger).PutByte(dal.CanonicalKeySepTransaction).PutUint64(id).Consume()
		require.NoError(t, session.SetBytes(key, nil))
		stamp := []uint64{30, 10, 20, 20}[i]
		put(readstore.TransactionTimestampKey(kb, ledger, stamp, id), nil)
		put(readstore.TransactionInsertedAtKey(kb, ledger, []uint64{20, 30, 10, 20}[i], id), nil)
		put(readstore.TransactionRevertedAtKey(kb, ledger, []uint64{20, 20, 30, 10}[i], id), txid(id))
		put(readstore.LedgerLogKey(kb, ledger, id), nil)
		put(readstore.LedgerLogDateKey(kb, ledger, stamp, id), nil)
	}
	for _, account := range []string{"users:a", "users:b", "world"} {
		key := kb.Reset().PutZonePrefix(dal.ZoneAttributes, dal.SubAttrMetadata).PutLedgerNameFixed(ledger).PutBytes([]byte(account)).PutByte(dal.CanonicalKeySepMetadata).PutBytes([]byte("name")).Consume()
		require.NoError(t, session.SetBytes(key, nil))
	}
	require.NoError(t, session.Commit())
	seedReversions(t, main, ledger, 2, 7)
	handle, err := main.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, handle.Close()) })
	for _, id := range []uint64{1, 5} {
		put(readstore.TransactionReferenceKey(kb, ledger, "ref", id), nil)
	}
	for _, row := range []struct {
		account string
		id      uint64
	}{{"users:a", 1}, {"users:a", 5}, {"users:b", 5}, {"users:b", 7}} {
		for _, prefix := range []byte{readstore.PrefixAccountTx, readstore.PrefixSourceAccountTx, readstore.PrefixDestinationAccountTx} {
			if prefix == readstore.PrefixSourceAccountTx && row.id == 7 || prefix == readstore.PrefixDestinationAccountTx && row.id == 1 {
				continue
			}
			put(readstore.AccountTxKey(kb, prefix, ledger, row.account, row.id), nil)
		}
	}
	for i, account := range []string{"users:a", "users:b"} {
		put(readstore.AccountByAssetKey(kb, ledger, "USD", 2, account), txid([]uint64{1, 7}[i]))
	}
	for _, id := range []uint64{1, 2, 5} {
		put(readstore.EntityExistsEventKeyV(kb, ledger, readstore.NamespaceTransaction, "tag", 1, id == 2, txid(id), 1, readstore.MetadataEventAdd), nil)
	}
	// A deletion after the pin must hide ID 5 only from the current view.
	put(readstore.EntityExistsEventKeyV(kb, ledger, readstore.NamespaceTransaction, "tag", 1, false, txid(5), 6, readstore.MetadataEventDel), nil)
	registry := staticIndexLookup{}
	for _, field := range []commonpb.TransactionBuiltinIndex{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS} {
		id := indexes.TxBuiltinID(field)
		registry[indexes.KeyFor(ledger, id)] = &commonpb.Index{Ledger: ledger, Id: id}
	}
	for _, id := range []*commonpb.IndexID{indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET), indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE), indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "tag")} {
		registry[indexes.KeyFor(ledger, id)] = &commonpb.Index{Ledger: ledger, Id: id}
	}
	schema := map[string]*commonpb.MetadataFieldSchema{"tag": {Type: commonpb.MetadataType_METADATA_TYPE_STRING}}
	ids := func(values ...uint64) [][]byte {
		var out [][]byte
		for _, id := range values {
			out = append(out, txid(id))
		}

		return out
	}
	accounts := func(values ...string) [][]byte {
		var out [][]byte
		for _, value := range values {
			out = append(out, []byte(value))
		}

		return out
	}
	builtin := func(field commonpb.TransactionBuiltinIndex, lo, hi uint64) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{Field: field, Cond: &commonpb.UintCondition{Min: &lo, Max: &hi, MaxExclusive: true}}}}
	}
	exists := func(null bool) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{Field: &commonpb.FieldRef{Metadata: "tag"}, Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{IncludeNull: null}}}}}
	}
	not := func(f *commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: f}}}
	}
	address := func(prefix bool, role commonpb.AddressRole) *commonpb.QueryFilter {
		a := &commonpb.AddressMatch{Role: role, Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: "users:a"}}
		if prefix {
			a.Match = &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "users:"}
		}

		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: a}}
	}
	tx := commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	logTarget := commonpb.QueryTarget_QUERY_TARGET_LOGS
	acct := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	type parityCase struct {
		name   string
		target commonpb.QueryTarget
		filter *commonpb.QueryFilter
		pin    uint64
		want   [][]byte
	}
	cases := []parityCase{
		{"transactions universe", tx, nil, 10, ids(1, 2, 5, 7)},
		{"not id range", tx, not(builtin(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, 2, 6)), 10, ids(1, 7)},
		{"nested not", tx, not(not(builtin(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, 2, 6))), 10, ids(2, 5)},
		{"reverted", tx, &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{Reverted: &commonpb.RevertedCondition{Value: true}}}, 10, ids(2, 7)},
		{"not reverted", tx, &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{Reverted: &commonpb.RevertedCondition{Value: false}}}, 10, ids(1, 5)},
		{"exists current", tx, exists(false), 10, ids(1)},
		{"exists pinned", tx, exists(false), 3, ids(1, 5)},
		{"exists including null", tx, exists(true), 10, ids(1, 2)},
		{"reference", tx, &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{Reference: &commonpb.ReferenceCondition{Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "ref"}}}}}, 10, ids(1, 5)},
		{"accounts universe", acct, nil, 10, accounts("users:a", "users:b", "world")},
		{"account address exact", acct, address(false, 0), 10, accounts("users:a")},
		{"account address prefix", acct, address(true, 0), 10, accounts("users:a", "users:b")},
		{"account not address", acct, not(address(true, 0)), 10, accounts("world")},
		{"account asset", acct, accountHasAssetFilter("USD", 2), 10, accounts("users:a", "users:b")},
		{"account asset pinned", acct, accountHasAssetFilter("USD", 2), 3, accounts("users:a")},
		{"logs universe", logTarget, nil, 10, ids(1, 2, 5, 7)},
		{"revertedAt pinned", tx, builtin(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT, 20, 30), 1, ids(1)},
		{"matching ledger", logTarget, &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Ledger{Ledger: &commonpb.LedgerCondition{Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ledger}}}}}, 10, ids(1, 2, 5, 7)},
		{"other ledger", logTarget, &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Ledger{Ledger: &commonpb.LedgerCondition{Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "other"}}}}}, 10, nil},
		{"logs date", logTarget, &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{LogBuiltinUint: &commonpb.LogBuiltinUintCondition{Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, Cond: &commonpb.UintCondition{Min: new(uint64(20)), Max: new(uint64(30)), MaxExclusive: true}}}}, 10, ids(5, 7)},
	}
	for _, field := range []commonpb.TransactionBuiltinIndex{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT} {
		want := ids(5, 7)
		switch field {
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
			want = ids(1, 7)
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT:
			want = ids(1, 2)
		}
		cases = append(cases, parityCase{field.String(), tx, builtin(field, 20, 30), 10, want})
	}
	for _, role := range []commonpb.AddressRole{commonpb.AddressRole_ADDRESS_ROLE_ANY, commonpb.AddressRole_ADDRESS_ROLE_SOURCE, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION} {
		exact, prefix := ids(1, 5), ids(1, 5, 7)
		switch role {
		case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
			prefix = ids(1, 5)
		case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
			exact, prefix = ids(5), ids(5, 7)
		}
		cases = append(cases, parityCase{"tx address exact " + role.String(), tx, address(false, role), 10, exact}, parityCase{"tx address prefix " + role.String(), tx, address(true, role), 10, prefix})
	}
	for _, bounds := range []struct {
		name   string
		lo, hi uint64
		want   [][]byte
	}{
		{"range", 2, 7, ids(2, 5)}, {"equality", 5, 6, ids(5)}, {"missing", 3, 4, nil}, {"empty", 7, 2, nil},
	} {
		cases = append(cases, parityCase{"tx id " + bounds.name, tx, builtin(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, bounds.lo, bounds.hi), 10, bounds.want},
			parityCase{"log id " + bounds.name, logTarget, &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{LogId: &commonpb.LogIdCondition{Cond: &commonpb.UintCondition{Min: &bounds.lo, Max: &bounds.hi, MaxExclusive: true}}}}, 10, bounds.want})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fwd, err := query.Compile(rs.DB(), dal.NewKeyBuilder(), tc.filter, tc.target, ledger, nil, schema, &commonpb.LedgerInfo{Name: ledger}, registry, nil, nil, handle, tc.pin)
			require.NoError(t, err)
			defer fwd.Close()
			var ascending [][]byte
			for fwd.Next() {
				ascending = append(ascending, bytes.Clone(fwd.Current()))
			}
			require.NoError(t, fwd.Err())
			require.Equal(t, tc.want, ascending)
			rev, err := query.CompileReverse(rs.DB(), dal.NewKeyBuilder(), tc.filter, tc.target, ledger, nil, schema, &commonpb.LedgerInfo{Name: ledger}, registry, nil, nil, handle, tc.pin)
			require.NoError(t, err)
			defer rev.Close()
			var descending [][]byte
			for rev.Next() {
				descending = append(descending, bytes.Clone(rev.Current()))
			}
			require.NoError(t, rev.Err())
			want := slices.Clone(tc.want)
			slices.Reverse(want)
			require.Equal(t, want, descending)
			// Cursor seeks are absolute, including after a complete traversal.
			for _, entity := range want {
				require.True(t, rev.SeekLE(entity))
				require.Equal(t, entity, rev.Current())
				require.True(t, rev.SeekLE(entity), "repeated seek must not consume a row")
				require.Equal(t, entity, rev.Current())
			}
			require.NoError(t, rev.Err())
		})
	}
}
