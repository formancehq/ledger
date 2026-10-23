package query_test

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// The retained address mappings outlive the account's last volume/metadata
// attribute. Compile and page the public query iterators over real Pebble rows;
// no account attribute is seeded for the historical address.
func TestCompileHistoricalAddressWithoutCurrentAccount(t *testing.T) {
	t.Parallel()

	store := parityStore(t)
	const address = "accounts:gone"
	batch := store.NewBatch()
	kb := dal.NewKeyBuilder()
	for _, row := range []struct {
		prefix byte
		id     uint64
	}{
		{readstore.PrefixAccountTx, 100},
		{readstore.PrefixAccountTx, 101},
		{readstore.PrefixDestinationAccountTx, 100}, // funding
		{readstore.PrefixSourceAccountTx, 101},      // draining
	} {
		require.NoError(t, batch.SetBytes(readstore.AccountTxKey(kb, row.prefix, parityLedger, address, row.id), nil))
	}
	// Transaction metadata is independent of current account metadata.
	id := make([]byte, 8)
	binary.BigEndian.PutUint64(id, 100)
	require.NoError(t, batch.SetBytes(readstore.MetadataIndexEventKeyV(kb,
		parityLedger, readstore.NamespaceTransaction, "colour", 1,
		readstore.EncodeString(nil, "blue"), id, 10, readstore.MetadataEventAdd), nil))
	require.NoError(t, batch.Commit())

	for _, reverse := range []bool{false, true} {
		name := "ascending"
		if reverse {
			name = "descending"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for _, tc := range []struct {
				name string
				role commonpb.AddressRole
				want []uint64
			}{
				{"any", commonpb.AddressRole_ADDRESS_ROLE_ANY, []uint64{100, 101}},
				{"source", commonpb.AddressRole_ADDRESS_ROLE_SOURCE, []uint64{101}},
				{"destination", commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, []uint64{100}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					want := slices.Clone(tc.want)
					if reverse {
						slices.Reverse(want)
					}
					filter := addressExactFilter(address, tc.role)
					require.Equal(t, want, historicalAddressPages(t, store, filter, nil, reverse))
					filter.GetAddress().Match = &commonpb.AddressMatch_ParamExact{ParamExact: "address"}
					params := map[string]*commonpb.ParameterValue{
						"address": {Value: &commonpb.ParameterValue_StringValue{StringValue: address}},
					}
					require.Equal(t, want, historicalAddressPages(t, store, filter, params, reverse))
				})
			}

			filter := addressExactFilter(address, commonpb.AddressRole_ADDRESS_ROLE_ANY)
			require.Equal(t, []uint64{100}, historicalAddressPages(t, store,
				andFilter(filter, referenceFilter(parityReferenceA)), nil, reverse))
			require.Equal(t, []uint64{100}, historicalAddressPages(t, store,
				andFilter(filter, stringFieldFilter("colour", "blue")), nil, reverse))
			require.Equal(t, []uint64{100}, historicalAddressPages(t, store,
				stringFieldFilter("colour", "blue"), nil, reverse))
			require.Equal(t, []uint64{101}, historicalAddressPages(t, store,
				andFilter(filter, notFilter(txIDEqualFilter(100))), nil, reverse))
			require.Empty(t, historicalAddressPages(t, store,
				addressExactFilter("unknown", commonpb.AddressRole_ADDRESS_ROLE_ANY), nil, reverse))
			// Prefix expansion still enumerates current accounts.
			require.Empty(t, historicalAddressPages(t, store,
				addressPrefixFilter(address, commonpb.AddressRole_ADDRESS_ROLE_ANY), nil, reverse))

			reader := store.DB()
			if reverse {
				iter, err := query.CompileReverse(reader, dal.NewKeyBuilder(), filter,
					commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger, nil, nil,
					parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
				require.NoError(t, err)
				defer iter.Close()
				require.False(t, iter.Next(), "historical mappings do not imply current account membership")
				require.NoError(t, iter.Err())
			} else {
				iter, err := query.Compile(reader, dal.NewKeyBuilder(), filter,
					commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger, nil, nil,
					parityInfo(), parityRegistry(), parityResolver(), nil, reader, parityPin)
				require.NoError(t, err)
				defer iter.Close()
				require.False(t, iter.Next(), "historical mappings do not imply current account membership")
				require.NoError(t, iter.Err())
			}
		})
	}
}

func historicalAddressPages(t *testing.T, store *readstore.Store, filter *commonpb.QueryFilter,
	params map[string]*commonpb.ParameterValue, reverse bool,
) []uint64 {
	t.Helper()

	reader := store.DB()
	registry := parityRegistry()
	metadataID := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "colour")
	registry[indexes.KeyFor(parityLedger, metadataID)] = &commonpb.Index{Ledger: parityLedger, Id: metadataID}
	var cursor []byte
	var ids []uint64
	for range 20 {
		var items [][]byte
		var more bool
		var err error
		if reverse {
			iter, compileErr := query.CompileReverse(reader, dal.NewKeyBuilder(), filter,
				commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, parityLedger, params,
				paritySchema(), parityInfo(), registry, parityResolver(), nil, reader, parityPin)
			require.NoError(t, compileErr)
			items, more, err = readstore.PaginateReverse(iter, 1, cursor)
			iter.Close()
		} else {
			iter, compileErr := query.Compile(reader, dal.NewKeyBuilder(), filter,
				commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, parityLedger, params,
				paritySchema(), parityInfo(), registry, parityResolver(), nil, reader, parityPin)
			require.NoError(t, compileErr)
			items, more, err = readstore.PaginateForward(iter, 1, cursor)
			iter.Close()
		}
		require.NoError(t, err)
		for _, item := range items {
			ids = append(ids, binary.BigEndian.Uint64(item))
		}
		if !more {
			return ids
		}
		require.NotEmpty(t, items)
		cursor = items[len(items)-1]
	}
	t.Fatal("historical address pagination did not terminate")

	return nil
}
