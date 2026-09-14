package query_test

import (
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Descending pagination parity (EN-1966).
//
// The oracle is the ascending drain reversed. For every filter shape, a FULL
// traversal in descending PAGES — each page resuming from the previous page's
// last id, exactly as the controller does — must equal that oracle: no
// duplicates, no omissions, at every page size and at the boundaries.
//
// Paging is what makes this more than a drain comparison: it exercises the
// cursor seek on every leaf and composite in the tree, which is where a
// mis-implemented descending Seek silently drops or repeats rows.

const (
	parityLedger = "ledger1"

	// The asset cell and references the has-asset and reference leaves scan.
	parityAsset          = "USD"
	parityAssetPrecision = uint8(2)
	parityReferenceA     = "ref-a"
	parityReferenceB     = "ref-b"

	// The account prefix the address cases select on: it matches
	// accounts:00..accounts:09 and excludes accounts:10, accounts:11, so a
	// prefix bound that runs off the end of the range fails here.
	parityAddressPrefix = "accounts:0"
	parityAddressExact  = "accounts:03"
)

func parityInfo() *commonpb.LedgerInfo { return &commonpb.LedgerInfo{Name: parityLedger} }

// parityResolver reports a live v1 keyspace with NO type binding, so
// compileFieldCondition validates against the declared schema rather than the
// retype-window binding. That keeps the fixture about direction, not about
// EN-1724's rewrite window, which has its own tests.
func parityResolver() readstore.IndexVersionResolver {
	return func(string) (readstore.ResolvedIndexVersion, bool, error) {
		return readstore.ResolvedIndexVersion{Version: 1}, true, nil
	}
}

// parityRegistry declares every index the parity filters touch as READY.
func parityRegistry() staticIndexLookup {
	reg := staticIndexLookup{}
	for _, id := range []*commonpb.IndexID{
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
		indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET),
		indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
	} {
		reg[indexes.KeyFor(parityLedger, id)] = &commonpb.Index{Ledger: parityLedger, Id: id}
	}

	for _, key := range []string{"colour", "size", "score", "flag", "note"} {
		id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
		reg[indexes.KeyFor(parityLedger, id)] = &commonpb.Index{Ledger: parityLedger, Id: id}
	}

	return reg
}

func paritySchema() map[string]*commonpb.MetadataFieldSchema {
	return map[string]*commonpb.MetadataFieldSchema{
		"colour": {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		"size":   {Type: commonpb.MetadataType_METADATA_TYPE_UINT64},
		"score":  {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
		"flag":   {Type: commonpb.MetadataType_METADATA_TYPE_BOOL},
		"note":   {Type: commonpb.MetadataType_METADATA_TYPE_STRING},
	}
}

// parityStore seeds one read store with accounts carrying colour/size
// metadata, written as index EVENTS at a sequence below the pin the tests use.
func parityStore(t *testing.T) *readstore.Store {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())

	store, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()

	// 12 accounts so several page sizes divide the set unevenly.
	for i := range 12 {
		account := fmt.Sprintf("accounts:%02d", i)

		colour := "red"
		if i%3 == 0 {
			colour = "blue"
		}

		require.NoError(t, batch.SetBytes(readstore.MetadataIndexEventKeyV(
			kb, parityLedger, readstore.NamespaceAccount, "colour", 1,
			readstore.EncodeString(nil, colour), []byte(account),
			uint64(10+i), readstore.MetadataEventAdd), nil))

		require.NoError(t, batch.SetBytes(readstore.MetadataIndexEventKeyV(
			kb, parityLedger, readstore.NamespaceAccount, "size", 1,
			readstore.EncodeUint64(nil, uint64(i%4)), []byte(account),
			uint64(10+i), readstore.MetadataEventAdd), nil))

		// Signed metadata: values straddle zero so the sign-bit XOR in
		// EncodeInt64 decides the descending order, not raw byte order.
		require.NoError(t, batch.SetBytes(readstore.MetadataIndexEventKeyV(
			kb, parityLedger, readstore.NamespaceAccount, "score", 1,
			readstore.EncodeInt64(nil, int64(i)-6), []byte(account),
			uint64(10+i), readstore.MetadataEventAdd), nil))

		require.NoError(t, batch.SetBytes(readstore.MetadataIndexEventKeyV(
			kb, parityLedger, readstore.NamespaceAccount, "flag", 1,
			readstore.EncodeBool(nil, i%2 == 0), []byte(account),
			uint64(10+i), readstore.MetadataEventAdd), nil))

		// Existence rows so exists / NOT compositions have a universe.
		require.NoError(t, batch.SetBytes(readstore.EntityExistsEventKeyV(
			kb, parityLedger, readstore.NamespaceAccount, "colour", 1,
			false, []byte(account), uint64(10+i), readstore.MetadataEventAdd), nil))

		// "note" splits its existence rows across the null and non-null
		// flags, so exists(includeNull) compiles to the two-arm OR rather
		// than collapsing to the same single scan as exists(colour).
		require.NoError(t, batch.SetBytes(readstore.EntityExistsEventKeyV(
			kb, parityLedger, readstore.NamespaceAccount, "note", 1,
			i%4 == 1, []byte(account), uint64(10+i), readstore.MetadataEventAdd), nil))
	}

	seedParityAccountVolumes(t, batch, kb)
	seedParityAccountAssets(t, batch, kb)
	seedParityTransactions(t, batch, kb)
	seedParityAddressTx(t, batch, kb)
	seedParityReferences(t, batch, kb)
	seedParityReversions(t, batch)
	seedParityLogs(t, batch, kb)

	require.NoError(t, batch.Commit())

	return store
}

// parityTxIDs / parityLogIDs are the entity sets the TRANSACTIONS and LOGS
// cases page over. Both are 12 wide like the account set, so the same page
// sizes divide them unevenly.
func parityTxIDs() []uint64 {
	ids := make([]uint64, 0, 12)
	for i := range 12 {
		ids = append(ids, uint64(100+i))
	}

	return ids
}

func parityLogIDs() []uint64 {
	ids := make([]uint64, 0, 12)
	for i := range 12 {
		ids = append(ids, uint64(200+i))
	}

	return ids
}

// seedParityAccountVolumes writes the main-store volume rows the ACCOUNTS
// universe scans. Without them the universe shape compares an empty
// descending drain against an empty ascending reference and passes for a
// reason unrelated to direction: the index event rows above feed the filtered
// leaves, not PebbleAccountIterator, which reads the attributes zone.
func seedParityAccountVolumes(t *testing.T, batch *dal.WriteSession, kb *dal.KeyBuilder) {
	t.Helper()

	for i := range 12 {
		account := fmt.Sprintf("accounts:%02d", i)

		key := kb.Reset().
			PutZonePrefix(dal.ZoneAttributes, dal.SubAttrVolume).
			PutBytes(domain.NewVolumeKey(parityLedger, account, "USD/2", "").Bytes()).
			Build()
		require.NoError(t, batch.SetBytes(key, []byte{1}))
	}
}

// seedParityTransactions writes the main-store transaction rows the
// TRANSACTIONS universe and the id-range leaf scan, plus the timestamp index
// rows the value-ordered fallback needs. The key is assembled from the
// canonical pieces (domain.TransactionKey under the attributes zone) rather
// than a hand-spelled layout, so a change to that layout breaks the build
// here instead of silently seeding rows no iterator can see.
func seedParityTransactions(t *testing.T, batch *dal.WriteSession, kb *dal.KeyBuilder) {
	t.Helper()

	for i, id := range parityTxIDs() {
		txKey := kb.Reset().
			PutZonePrefix(dal.ZoneAttributes, dal.SubAttrTransaction).
			PutBytes(domain.TransactionKey{LedgerName: parityLedger, ID: id}.Bytes()).
			Build()
		require.NoError(t, batch.SetBytes(txKey, []byte{1}))

		// Timestamps ascend with the id, so (timestamp, entity) order and
		// entity order agree; the fallback is still exercised because the
		// scan spans several value buckets.
		require.NoError(t, batch.SetBytes(readstore.TransactionTimestampKey(
			dal.NewKeyBuilder(), parityLedger, uint64(1_000+i), id), nil))

		// inserted_at is the same fallback class over a different prefix, so
		// a bound built against the wrong index shows up here and not in the
		// timestamp case.
		require.NoError(t, batch.SetBytes(readstore.TransactionInsertedAtKey(
			dal.NewKeyBuilder(), parityLedger, uint64(2_000+i), id), nil))
	}
}

// seedParityLogs writes the ledger-log rows behind the LOGS universe and the
// log-id leaves, plus the log-date index rows the value-ordered fallback
// scans.
func seedParityLogs(t *testing.T, batch *dal.WriteSession, kb *dal.KeyBuilder) {
	t.Helper()

	for i, id := range parityLogIDs() {
		require.NoError(t, batch.SetBytes(readstore.LedgerLogKey(kb, parityLedger, id), nil))

		require.NoError(t, batch.SetBytes(readstore.LedgerLogDateKey(
			dal.NewKeyBuilder(), parityLedger, uint64(3_000+i), id), nil))
	}
}

// seedParityAccountAssets writes the account-by-asset rows behind the
// has-asset leaf. The value is the first touch's fold sequence, and the scan
// is stamp-gated in both directions: accounts:01's row carries a sequence
// ABOVE parityPin, so a direction that forgets the gate serves a row the
// other hides — a bug the set-vs-set oracle can only see because one row is
// deliberately out of view.
func seedParityAccountAssets(t *testing.T, batch *dal.WriteSession, kb *dal.KeyBuilder) {
	t.Helper()

	stamp := func(seq uint64) []byte {
		v := make([]byte, 8)
		binary.BigEndian.PutUint64(v, seq)

		return v
	}

	for i := range 12 {
		account := fmt.Sprintf("accounts:%02d", i)

		switch {
		case i == 1:
			require.NoError(t, batch.SetBytes(
				readstore.AccountByAssetKey(kb, parityLedger, parityAsset, parityAssetPrecision, account),
				stamp(parityPin+5)))
		case i%2 == 0:
			require.NoError(t, batch.SetBytes(
				readstore.AccountByAssetKey(kb, parityLedger, parityAsset, parityAssetPrecision, account),
				stamp(uint64(10+i))))
		}
	}
}

// seedParityAddressTx writes the account→transaction rows the address union
// walks. Each transaction is linked to the account of the same rank, so the
// prefix "accounts:0" selects ten of the twelve; accounts:00 additionally
// owns transaction 105, so the union must deduplicate an id reachable from
// two accounts inside one scan rather than emit it twice.
func seedParityAddressTx(t *testing.T, batch *dal.WriteSession, kb *dal.KeyBuilder) {
	t.Helper()

	ids := parityTxIDs()

	for i, id := range ids {
		account := fmt.Sprintf("accounts:%02d", i)

		require.NoError(t, batch.SetBytes(
			readstore.AccountTxKey(kb, readstore.PrefixAccountTx, parityLedger, account, id), nil))

		if i%3 == 0 {
			require.NoError(t, batch.SetBytes(
				readstore.AccountTxKey(kb, readstore.PrefixSourceAccountTx, parityLedger, account, id), nil))
		}
	}

	require.NoError(t, batch.SetBytes(
		readstore.AccountTxKey(kb, readstore.PrefixAccountTx, parityLedger, "accounts:00", ids[5]), nil))
}

// seedParityReferences writes the transaction-reference index rows behind the
// reverse txref prefix scan.
func seedParityReferences(t *testing.T, batch *dal.WriteSession, kb *dal.KeyBuilder) {
	t.Helper()

	for i, id := range parityTxIDs() {
		reference := parityReferenceB
		if i%3 == 0 {
			reference = parityReferenceA
		}

		require.NoError(t, batch.SetBytes(
			readstore.TransactionReferenceKey(kb, parityLedger, reference, id), nil))
	}
}

// seedParityReversions writes the reversion bitset words behind the reverted
// leaf. Layout is the one ReadReversionBitset scans:
// [ZonePerLedger][SubPLReversions][ledger padded 64B][word index BE 8B] with
// a little-endian word value.
func seedParityReversions(t *testing.T, batch *dal.WriteSession) {
	t.Helper()

	bs := &bitset.Bitset{}
	for _, id := range parityRevertedTxIDs() {
		bs.Set(id)
	}

	for word, value := range bs.Words() {
		if value == 0 {
			continue
		}

		key := make([]byte, 2+dal.LedgerNameFixedSize+8)
		key[0] = dal.ZonePerLedger
		key[1] = dal.SubPLReversions
		copy(key[2:], parityLedger)
		binary.BigEndian.PutUint64(key[2+dal.LedgerNameFixedSize:], uint64(word))

		v := make([]byte, 8)
		binary.LittleEndian.PutUint64(v, value)

		require.NoError(t, batch.SetBytes(key, v))
	}
}

// parityRevertedTxIDs are the transactions the reversion bitset marks.
func parityRevertedTxIDs() []uint64 {
	ids := parityTxIDs()

	return []uint64{ids[1], ids[4], ids[7]}
}

func accountFieldFilter(key string, cond *commonpb.FieldCondition) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: cond}}
}

func stringFieldFilter(key, value string) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_StringCond{StringCond: &commonpb.StringCondition{
			Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value},
		}},
	})
}

func uintRangeFieldFilter(key string, minV, maxV uint64) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_UintCond{UintCond: &commonpb.UintCondition{
			Min: &minV, Max: &maxV,
		}},
	})
}

func uintEqualFieldFilter(key string, v uint64) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_UintCond{UintCond: &commonpb.UintCondition{
			Min: &v, Max: &v,
		}},
	})
}

func existsFieldFilter(key string) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field:     &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
	})
}

// existsWithNullFieldFilter is the two-arm form: it compiles to an OR over
// the non-null and null existence scans instead of a single scan.
func existsWithNullFieldFilter(key string) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{
			IncludeNull: true,
		}},
	})
}

func intEqualFieldFilter(key string, v int64) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_IntCond{IntCond: &commonpb.IntCondition{
			Min: &v, Max: &v,
		}},
	})
}

func intRangeFieldFilter(key string, minV, maxV int64) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_IntCond{IntCond: &commonpb.IntCondition{
			Min: &minV, Max: &maxV,
		}},
	})
}

func boolFieldFilter(key string, v bool) *commonpb.QueryFilter {
	return accountFieldFilter(key, &commonpb.FieldCondition{
		Field: &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_BoolCond{BoolCond: &commonpb.BoolCondition{
			Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: v},
		}},
	})
}

// --- Address, reference, reverted, has-asset and log-date constructors -----

func addressPrefixFilter(addrPrefix string, role commonpb.AddressRole) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{
		Address: &commonpb.AddressMatch{
			Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: addrPrefix},
			Role:  role,
		},
	}}
}

func addressExactFilter(addr string, role commonpb.AddressRole) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{
		Address: &commonpb.AddressMatch{
			Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: addr},
			Role:  role,
		},
	}}
}

func referenceFilter(reference string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{
		Reference: &commonpb.ReferenceCondition{
			Cond: &commonpb.StringCondition{
				Value: &commonpb.StringCondition_Hardcoded{Hardcoded: reference},
			},
		},
	}}
}

func revertedFilter(value bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{
		Reverted: &commonpb.RevertedCondition{Value: value},
	}}
}

func hasAssetFilter(assetBase string, precision uint32) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_AccountHasAsset{
		AccountHasAsset: &commonpb.AccountHasAssetCondition{
			AssetBase: assetBase,
			Precision: precision,
		},
	}}
}

func txInsertedAtRangeFilter(minV, maxV uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
		BuiltinUint: &commonpb.BuiltinUintCondition{
			Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT,
			Cond:  &commonpb.UintCondition{Min: &minV, Max: &maxV},
		},
	}}
}

func logDateRangeFilter(minV, maxV uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{
		LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
			Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
			Cond:  &commonpb.UintCondition{Min: &minV, Max: &maxV},
		},
	}}
}

func andFilter(fs ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: fs}}}
}

func orFilter(fs ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: fs}}}
}

func notFilter(f *commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: f}}}
}

const parityPin = 1000

// ascendingReference drains the ascending tree — the oracle's source.
func ascendingReference(t *testing.T, store *readstore.Store, target commonpb.QueryTarget, filter *commonpb.QueryFilter) []string {
	t.Helper()

	reader := store.DB()

	iter, err := query.Compile(
		reader, dal.NewKeyBuilder(), filter,
		target, parityLedger,
		nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
	require.NoError(t, err)

	defer iter.Close()

	var out []string
	for iter.Next() {
		out = append(out, string(iter.Current()))
	}

	require.NoError(t, iter.Err())

	return out
}

// descendingByPages walks the whole result descending, one page at a time,
// resuming from the previous page's last entity — the controller's own loop.
func descendingByPages(t *testing.T, store *readstore.Store, target commonpb.QueryTarget, filter *commonpb.QueryFilter, pageSize uint32) []string {
	t.Helper()

	reader := store.DB()

	var (
		out    []string
		before []byte
	)

	// A generous page budget: any traversal needing more than this is looping.
	for range 100 {
		iter, err := query.CompileReverse(
			reader, dal.NewKeyBuilder(), filter,
			target, parityLedger,
			nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
		require.NoError(t, err)

		items, _, pErr := readstore.PaginateReverse(iter, pageSize, before)
		iter.Close()
		require.NoError(t, pErr)

		if len(items) == 0 {
			return out
		}

		for _, it := range items {
			out = append(out, string(it))
		}

		before = items[len(items)-1]
	}

	t.Fatal("descending traversal did not terminate: a page is repeating its cursor")

	return nil
}

// --- TRANSACTIONS and LOGS filter constructors -----------------------------

func txIDRangeFilter(minV, maxV uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
		BuiltinUint: &commonpb.BuiltinUintCondition{
			Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID,
			Cond:  &commonpb.UintCondition{Min: &minV, Max: &maxV},
		},
	}}
}

func txIDEqualFilter(id uint64) *commonpb.QueryFilter { return txIDRangeFilter(id, id) }

func txTimestampRangeFilter(minV, maxV uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
		BuiltinUint: &commonpb.BuiltinUintCondition{
			Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
			Cond:  &commonpb.UintCondition{Min: &minV, Max: &maxV},
		},
	}}
}

func logIDRangeFilter(minV, maxV uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{
		LogId: &commonpb.LogIdCondition{
			Cond: &commonpb.UintCondition{Min: &minV, Max: &maxV},
		},
	}}
}

func logIDEqualFilter(id uint64) *commonpb.QueryFilter { return logIDRangeFilter(id, id) }

// parityCase is one (target, filter) pair the oracle drives. Target is part of
// the case and not a fixed constant: the motivating surface for EN-1966 is
// TRANSACTIONS, the public default descending direction, and an
// ACCOUNTS-only oracle proves the acceptance criterion on the wrong target.
type parityCase struct {
	name   string
	target commonpb.QueryTarget
	filter *commonpb.QueryFilter
}

// parityCases is the three-target matrix: every target crossed with the
// filter families reachable on it, streaming leaves and materializing
// fallbacks alike, plus the boolean compositions over them.
func parityCases() []parityCase {
	cases := make([]parityCase, 0, 24)

	for _, f := range accountParityFilters() {
		cases = append(cases, parityCase{
			name:   "accounts/" + f.name,
			target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			filter: f.filter,
		})
	}

	txIDs := parityTxIDs()
	lo, hi := txIDs[0], txIDs[len(txIDs)-1]

	for _, f := range []struct {
		name   string
		filter *commonpb.QueryFilter
	}{
		{"universe", nil},
		// The bounded id range is the leaf this matrix exists for: it is the
		// only compiled descending path that reaches
		// NewPebbleReverseTxRangeIterator.
		{"tx id range (streaming leaf)", txIDRangeFilter(lo+2, hi-2)},
		{"tx id range open above", txIDRangeFilter(lo+5, ^uint64(0))},
		{"tx id equality", txIDEqualFilter(lo + 3)},
		{"tx timestamp range (materializing fallback)", txTimestampRangeFilter(1_002, 1_008)},
		{"and of two id ranges", andFilter(txIDRangeFilter(lo, hi-1), txIDRangeFilter(lo+4, hi))},
		{"or of two id equalities", orFilter(txIDEqualFilter(lo+1), txIDEqualFilter(hi-1))},
		{"not of an id equality", notFilter(txIDEqualFilter(lo + 6))},
		{"and containing a materializing range", andFilter(txIDRangeFilter(lo, hi), txTimestampRangeFilter(1_003, 1_009))},
		{"empty result", txIDEqualFilter(999_999)},
		// The remaining leaf classes reachable on TRANSACTIONS: the
		// account→tx union (both match forms and a role bucket), the txref
		// reverse prefix, the reversion bitset and its complement, and the
		// inserted_at arm of the timestamp fallback.
		{"address prefix (materializing union)", addressPrefixFilter(parityAddressPrefix, commonpb.AddressRole_ADDRESS_ROLE_ANY)},
		{"address exact (materializing union)", addressExactFilter(parityAddressExact, commonpb.AddressRole_ADDRESS_ROLE_ANY)},
		{"address prefix on the source role bucket", addressPrefixFilter(parityAddressPrefix, commonpb.AddressRole_ADDRESS_ROLE_SOURCE)},
		{"reference (streaming prefix leaf)", referenceFilter(parityReferenceA)},
		{"reverted true (bitset leaf)", revertedFilter(true)},
		{"reverted false (not over the bitset)", revertedFilter(false)},
		{"inserted_at range (materializing fallback)", txInsertedAtRangeFilter(2_002, 2_008)},
		{"and of address prefix and id range", andFilter(
			addressPrefixFilter(parityAddressPrefix, commonpb.AddressRole_ADDRESS_ROLE_ANY),
			txIDRangeFilter(lo+3, hi),
		)},
		{"or of reference and reverted", orFilter(referenceFilter(parityReferenceA), revertedFilter(true))},
	} {
		cases = append(cases, parityCase{
			name:   "transactions/" + f.name,
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			filter: f.filter,
		})
	}

	logIDs := parityLogIDs()
	logLo, logHi := logIDs[0], logIDs[len(logIDs)-1]

	for _, f := range []struct {
		name   string
		filter *commonpb.QueryFilter
	}{
		{"universe", nil},
		{"log id range (materializing fallback)", logIDRangeFilter(logLo+2, logHi-2)},
		{"log id equality", logIDEqualFilter(logLo + 4)},
		{"or of two id equalities", orFilter(logIDEqualFilter(logLo+1), logIDEqualFilter(logHi-1))},
		{"and of two id ranges", andFilter(logIDRangeFilter(logLo, logHi-1), logIDRangeFilter(logLo+3, logHi))},
		{"empty result", logIDEqualFilter(999_999)},
		// The log-date arm of the timestamp fallback: a different prefix and
		// no stamp gate, so it cannot ride on the transaction cases.
		{"log date range (materializing fallback)", logDateRangeFilter(3_002, 3_008)},
		{"and of log date and id range", andFilter(
			logDateRangeFilter(3_001, 3_010), logIDRangeFilter(logLo+4, logHi),
		)},
	} {
		cases = append(cases, parityCase{
			name:   "logs/" + f.name,
			target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
			filter: f.filter,
		})
	}

	return cases
}

func accountParityFilters() []struct {
	name   string
	filter *commonpb.QueryFilter
} {
	return []struct {
		name   string
		filter *commonpb.QueryFilter
	}{
		{"universe", nil},
		{"string equality (streaming leaf)", stringFieldFilter("colour", "red")},
		{"uint equality (streaming leaf)", uintEqualFieldFilter("size", 2)},
		{"uint range (materializing fallback)", uintRangeFieldFilter("size", 1, 2)},
		{"exists (streaming leaf)", existsFieldFilter("colour")},
		{"and of two streaming leaves", andFilter(stringFieldFilter("colour", "red"), uintEqualFieldFilter("size", 1))},
		{"or of two streaming leaves", orFilter(stringFieldFilter("colour", "blue"), uintEqualFieldFilter("size", 3))},
		{"not of a streaming leaf", notFilter(stringFieldFilter("colour", "red"))},
		{"and containing a materializing range", andFilter(stringFieldFilter("colour", "red"), uintRangeFieldFilter("size", 0, 2))},
		{"nested and(or, not)", andFilter(
			orFilter(stringFieldFilter("colour", "red"), stringFieldFilter("colour", "blue")),
			notFilter(uintEqualFieldFilter("size", 0)),
		)},
		{"nested not(not)", notFilter(notFilter(stringFieldFilter("colour", "blue")))},
		{"empty result", stringFieldFilter("colour", "chartreuse")},
		// The remaining leaf classes reachable on ACCOUNTS. Without these the
		// oracle proves the criterion only for the string/uint metadata
		// leaves, leaving the address prefix, the exact-address slice, the
		// signed encoding and the stamp-gated has-asset scan unproven at the
		// compiled paged level.
		{"int equality (streaming leaf)", intEqualFieldFilter("score", -3)},
		{"int range (materializing fallback)", intRangeFieldFilter("score", -4, 2)},
		{"bool equality (streaming leaf)", boolFieldFilter("flag", true)},
		{"exists with null (or of two arms)", existsWithNullFieldFilter("note")},
		{"address prefix (streaming leaf)", addressPrefixFilter(parityAddressPrefix, commonpb.AddressRole_ADDRESS_ROLE_ANY)},
		{"address exact (slice leaf)", addressExactFilter(parityAddressExact, commonpb.AddressRole_ADDRESS_ROLE_ANY)},
		{"has asset (stamp-gated leaf)", hasAssetFilter(parityAsset, uint32(parityAssetPrecision))},
		{"and of address prefix and has asset", andFilter(
			addressPrefixFilter(parityAddressPrefix, commonpb.AddressRole_ADDRESS_ROLE_ANY),
			hasAssetFilter(parityAsset, uint32(parityAssetPrecision)),
		)},
		{"not of has asset", notFilter(hasAssetFilter(parityAsset, uint32(parityAssetPrecision)))},
	}
}

// TestDescendingParity_FullTraversal is the acceptance oracle: for every
// target, filter shape and page size, the paged descending traversal equals
// the reversed ascending reference.
func TestDescendingParity_FullTraversal(t *testing.T) {
	t.Parallel()

	store := parityStore(t)

	for _, tc := range parityCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			want := ascendingReference(t, store, tc.target, tc.filter)
			slices.Reverse(want)

			// Page sizes around, at, and past the result size, so boundary
			// and final pages are all exercised.
			for _, pageSize := range []uint32{1, 2, 3, 5, 12, 50} {
				got := descendingByPages(t, store, tc.target, tc.filter, pageSize)

				require.Equal(t, want, got,
					"pageSize=%d: descending traversal must equal the reversed ascending reference", pageSize)

				require.Equal(t, len(want), len(got),
					"pageSize=%d: no duplicates and no omissions", pageSize)
			}
		})
	}
}

// TestDescendingParity_EveryTargetIsCovered fails if a supported target drops
// out of the matrix. Without it, deleting the TRANSACTIONS cases would leave
// the oracle green while proving nothing about the default descending
// surface — the exact hole this matrix was added to close.
func TestDescendingParity_EveryTargetIsCovered(t *testing.T) {
	t.Parallel()

	seen := map[commonpb.QueryTarget]int{}
	for _, tc := range parityCases() {
		seen[tc.target]++
	}

	for _, target := range []commonpb.QueryTarget{
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		commonpb.QueryTarget_QUERY_TARGET_LOGS,
	} {
		require.GreaterOrEqual(t, seen[target], 4,
			"target %s needs paged descending parity cases, not fewer than four",
			commonpb.TargetHumanName(target))
	}
}

// TestDescendingParity_NonEmptyFixtures guards the matrix against the failure
// mode that makes every case above pass for the wrong reason: an unseeded
// target compiles fine and yields an empty reference, so "descending equals
// reversed ascending" holds trivially.
func TestDescendingParity_NonEmptyFixtures(t *testing.T) {
	t.Parallel()

	store := parityStore(t)

	for _, tc := range []parityCase{
		{"accounts", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil},
		{"transactions", commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, nil},
		{"logs", commonpb.QueryTarget_QUERY_TARGET_LOGS, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Len(t, ascendingReference(t, store, tc.target, tc.filter), 12,
				"the %s fixture must be seeded, or every parity case passes on an empty set", tc.name)
		})
	}
}

// TestDescendingParity_LeafFixtureSizes is TestDescendingParity_NonEmptyFixtures
// at leaf granularity. A seeded target is not enough: each leaf class in the
// matrix reads its OWN index rows, so a case whose rows are missing or written
// under the wrong prefix still compiles, still yields an empty reference, and
// still passes "descending equals reversed ascending" — proving nothing about
// the leaf it was added for. Pinning the exact size means a mis-seeded family
// fails here, naming the family, instead of quietly weakening the oracle.
func TestDescendingParity_LeafFixtureSizes(t *testing.T) {
	t.Parallel()

	store := parityStore(t)

	accounts := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	transactions := commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	logs := commonpb.QueryTarget_QUERY_TARGET_LOGS

	for _, tc := range []struct {
		name   string
		target commonpb.QueryTarget
		filter *commonpb.QueryFilter
		want   int
	}{
		{"accounts/int equality", accounts, intEqualFieldFilter("score", -3), 1},
		{"accounts/int range", accounts, intRangeFieldFilter("score", -4, 2), 7},
		{"accounts/bool equality", accounts, boolFieldFilter("flag", true), 6},
		{"accounts/exists with null", accounts, existsWithNullFieldFilter("note"), 12},
		{"accounts/address prefix", accounts, addressPrefixFilter(parityAddressPrefix, commonpb.AddressRole_ADDRESS_ROLE_ANY), 10},
		{"accounts/address exact", accounts, addressExactFilter(parityAddressExact, commonpb.AddressRole_ADDRESS_ROLE_ANY), 1},
		// Six accounts carry a stamped has-asset row at or below the pin;
		// accounts:01's row is stamped above it and must stay hidden. A
		// count of seven here means the gate was dropped.
		{"accounts/has asset (gated)", accounts, hasAssetFilter(parityAsset, uint32(parityAssetPrecision)), 6},
		{"transactions/address prefix", transactions, addressPrefixFilter(parityAddressPrefix, commonpb.AddressRole_ADDRESS_ROLE_ANY), 10},
		{"transactions/address exact", transactions, addressExactFilter(parityAddressExact, commonpb.AddressRole_ADDRESS_ROLE_ANY), 1},
		{"transactions/address source role", transactions, addressPrefixFilter(parityAddressPrefix, commonpb.AddressRole_ADDRESS_ROLE_SOURCE), 4},
		{"transactions/reference", transactions, referenceFilter(parityReferenceA), 4},
		{"transactions/reverted true", transactions, revertedFilter(true), len(parityRevertedTxIDs())},
		{"transactions/reverted false", transactions, revertedFilter(false), 12 - len(parityRevertedTxIDs())},
		{"transactions/inserted_at range", transactions, txInsertedAtRangeFilter(2_002, 2_008), 7},
		{"logs/log date range", logs, logDateRangeFilter(3_002, 3_008), 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Len(t, ascendingReference(t, store, tc.target, tc.filter), tc.want,
				"the %s fixture must select exactly %d entities, or its parity case proves nothing about that leaf",
				tc.name, tc.want)
		})
	}
}

// TestDescendingParity_CursorBoundaries pins the two ends of the cursor: a
// cursor at the highest entity must exclude it and return the rest, and a
// cursor at the lowest must return nothing.
func TestDescendingParity_CursorBoundaries(t *testing.T) {
	t.Parallel()

	store := parityStore(t)
	reader := store.DB()
	filter := stringFieldFilter("colour", "red")

	want := ascendingReference(t, store, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, filter)
	slices.Reverse(want)
	require.NotEmpty(t, want)

	page := func(before []byte, size uint32) []string {
		iter, err := query.CompileReverse(
			reader, dal.NewKeyBuilder(), filter,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger,
			nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
		require.NoError(t, err)

		defer iter.Close()

		items, _, pErr := readstore.PaginateReverse(iter, size, before)
		require.NoError(t, pErr)

		out := make([]string, 0, len(items))
		for _, it := range items {
			out = append(out, string(it))
		}

		return out
	}

	require.Equal(t, want, page(nil, 50), "no cursor returns the whole set descending")

	require.Equal(t, want[1:], page([]byte(want[0]), 50),
		"a cursor at the highest entity excludes it")

	require.Empty(t, page([]byte(want[len(want)-1]), 50),
		"a cursor at the lowest entity returns nothing")
}

// TestDescendingParity_NoFullMaterialization is the performance contract in
// test form. A streaming shape must not materialize anything to serve a
// descending page, and a fallback shape must materialize its ONE range — not
// two, and not a second copy for the reversal.
func TestDescendingParity_NoFullMaterialization(t *testing.T) {
	t.Parallel()

	store := parityStore(t)
	reader := store.DB()

	profileFor := func(filter *commonpb.QueryFilter) *query.QueryProfile {
		profile := &query.QueryProfile{}

		iter, err := query.CompileReverse(
			reader, dal.NewKeyBuilder(), filter,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger,
			nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), profile, reader, parityPin)
		require.NoError(t, err)

		defer iter.Close()

		items, _, pErr := readstore.PaginateReverse(iter, 2, nil)
		require.NoError(t, pErr)
		require.Len(t, items, 2)

		return profile
	}

	streaming := profileFor(andFilter(stringFieldFilter("colour", "red"), existsFieldFilter("colour")))
	require.Zero(t, streaming.MaterializedRanges,
		"a composition of streaming leaves must serve a descending page without materializing")
	require.Zero(t, streaming.MaterializedItems)

	fallback := profileFor(uintRangeFieldFilter("size", 1, 2))
	require.Equal(t, 1, fallback.MaterializedRanges,
		"the value-ordered range keeps its ONE materialization")

	ascProfile := &query.QueryProfile{}
	ascIter, err := query.Compile(
		reader, dal.NewKeyBuilder(), uintRangeFieldFilter("size", 1, 2),
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger,
		nil, paritySchema(), parityInfo(), parityRegistry(), parityResolver(), ascProfile, reader, parityPin)
	require.NoError(t, err)

	for ascIter.Next() {
	}

	require.NoError(t, ascIter.Err())
	ascIter.Close()

	require.Equal(t, ascProfile.MaterializedItems, fallback.MaterializedItems,
		"descending must reuse the ascending materialization, not double it")
}

// TestDescendingParity_Uint64Extremes covers the fixed-width entity
// boundaries: id 0 and MaxUint64 must page like any other id.
func TestDescendingParity_Uint64Extremes(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())

	store, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()

	ids := []uint64{0, 1, 1 << 32, ^uint64(0) - 1, ^uint64(0)}
	for i, id := range ids {
		idBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(idBytes, id)
		require.NoError(t, batch.SetBytes(readstore.LedgerLogKey(kb, parityLedger, id), nil))
		_ = i
	}

	require.NoError(t, batch.Commit())

	reader := store.DB()

	prefix := readstore.LedgerLogPrefix(dal.NewKeyBuilder(), parityLedger)

	rev, err := readstore.NewReversePrefixIterator(reader, prefix, len(prefix), 8)
	require.NoError(t, err)

	defer rev.Close()

	var before []byte

	var got []uint64

	for range 20 {
		items, _, pErr := readstore.PaginateReverse(rev, 2, before)
		require.NoError(t, pErr)

		if len(items) == 0 {
			break
		}

		for _, it := range items {
			got = append(got, binary.BigEndian.Uint64(it))
		}

		before = items[len(items)-1]
	}

	want := slices.Clone(ids)
	slices.Reverse(want)

	require.Equal(t, want, got,
		"descending paging must cross id 0 and MaxUint64 without dropping or repeating")
}
