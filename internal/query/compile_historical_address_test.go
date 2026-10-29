package query_test

import (
	"encoding/binary"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Exact transaction filters use retained mappings even after all current account
// attributes are purged; surviving metadata or volumes do not change history.
func TestCompileHistoricalAddressIndependentOfCurrentAccount(t *testing.T) {
	t.Parallel()
	for _, residency := range []string{"absent", "metadata", "volume"} {
		t.Run(residency, func(t *testing.T) {
			t.Parallel()
			store := parityStore(t)
			const address = "accounts:ephemeral"
			batch := store.NewBatch()
			kb := dal.NewKeyBuilder()
			for _, row := range []struct {
				prefix byte
				id     uint64
			}{
				{readstore.PrefixAccountTx, 100},
				{readstore.PrefixAccountTx, 101},
				{readstore.PrefixDestinationAccountTx, 100},
				{readstore.PrefixSourceAccountTx, 101},
			} {
				require.NoError(t, batch.SetBytes(readstore.AccountTxKey(kb, row.prefix, parityLedger, address, row.id), nil))
			}
			switch residency {
			case "metadata":
				key := kb.Reset().PutZonePrefix(dal.ZoneAttributes, dal.SubAttrMetadata).
					PutBytes((domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: parityLedger, Account: address}, Key: "owner"}).Bytes()).Build()
				require.NoError(t, batch.SetBytes(key, []byte{1}))
			case "volume":
				key := kb.Reset().PutZonePrefix(dal.ZoneAttributes, dal.SubAttrVolume).
					PutBytes(domain.NewVolumeKey(parityLedger, address, "EUR", "").Bytes()).Build()
				require.NoError(t, batch.SetBytes(key, []byte{1}))
			}
			id := make([]byte, 8)
			binary.BigEndian.PutUint64(id, 100)
			require.NoError(t, batch.SetBytes(readstore.MetadataIndexEventKeyV(kb,
				parityLedger, readstore.NamespaceTransaction, "colour", 1,
				readstore.EncodeString(nil, "blue"), id, 10, readstore.MetadataEventAdd), nil))
			require.NoError(t, batch.Commit())
			for _, reverse := range []bool{false, true} {
				// Account-target membership still requires current attributes.
				var accountIter interface {
					Next() bool
					Current() []byte
					Err() error
					Close()
				}
				var err error
				filter := addressExactFilter(address, commonpb.AddressRole_ADDRESS_ROLE_ANY)
				if reverse {
					accountIter, err = query.CompileReverse(store.DB(), dal.NewKeyBuilder(), filter,
						commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger, nil,
						paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, store.DB(), parityPin)
				} else {
					accountIter, err = query.Compile(store.DB(), dal.NewKeyBuilder(), filter,
						commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, parityLedger, nil,
						paritySchema(), parityInfo(), parityRegistry(), parityResolver(), nil, store.DB(), parityPin)
				}
				require.NoError(t, err)
				hasAccount := accountIter.Next()
				require.Equal(t, residency != "absent", hasAccount)
				if hasAccount {
					require.Equal(t, address, string(accountIter.Current()))
					require.False(t, accountIter.Next())
				}
				require.NoError(t, accountIter.Err())
				accountIter.Close()
				for _, tc := range []struct {
					role commonpb.AddressRole
					want []uint64
				}{
					{commonpb.AddressRole_ADDRESS_ROLE_ANY, []uint64{100, 101}},
					{commonpb.AddressRole_ADDRESS_ROLE_SOURCE, []uint64{101}},
					{commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, []uint64{100}},
				} {
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
				}
				filter = addressExactFilter(address, commonpb.AddressRole_ADDRESS_ROLE_ANY)
				wantFunding, wantDraining := []uint64{100}, []uint64{101}
				require.Equal(t, wantFunding, historicalAddressPages(t, store,
					andFilter(filter, referenceFilter(parityReferenceA)), nil, reverse))
				require.Equal(t, wantFunding, historicalAddressPages(t, store,
					andFilter(filter, stringFieldFilter("colour", "blue")), nil, reverse))
				require.Equal(t, []uint64{100}, historicalAddressPages(t, store,
					stringFieldFilter("colour", "blue"), nil, reverse))
				require.Equal(t, wantDraining, historicalAddressPages(t, store,
					andFilter(filter, notFilter(txIDEqualFilter(100))), nil, reverse))
				require.Empty(t, historicalAddressPages(t, store,
					addressExactFilter("unknown", commonpb.AddressRole_ADDRESS_ROLE_ANY), nil, reverse))
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
