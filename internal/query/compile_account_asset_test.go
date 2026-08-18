package query_test

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// staticIndexLookup is an in-memory indexes.Lookup for the account-by-asset
// compile tests. Keyed exactly like the production registry.
type staticIndexLookup map[domain.IndexKey]*commonpb.Index

func (s staticIndexLookup) Get(key domain.IndexKey) (commonpb.IndexReader, error) {
	idx, ok := s[key]
	if !ok {
		return nil, domain.ErrNotFound
	}

	return idx.AsReader(), nil
}

func accountHasAssetFilter(assetBase string, precision uint32) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_AccountHasAsset{
		AccountHasAsset: &commonpb.AccountHasAssetCondition{
			AssetBase: assetBase,
			Precision: precision,
		},
	}}
}

// TestCompile_AccountHasAsset_RequiresReady pins the READY gate: a
// has-asset condition on the ACCOUNTS path must refuse with
// ErrIndexBuilding when the local replica's IndexVersionState reports
// CurrentVersion == 0 (initial backfill not yet flipped into a live
// keyspace). There is NO on-scan fallback.
func TestCompile_AccountHasAsset_RequiresReady(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"

	assetID := indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)
	info := &commonpb.LedgerInfo{Name: ledgerName}

	// Index IS declared, but the local replica reports CurrentVersion == 0.
	registry := staticIndexLookup{
		indexes.KeyFor(ledgerName, assetID): {Ledger: ledgerName, Id: assetID},
	}
	resolverZero := func(string) (uint32, bool, error) { return 0, true, nil }

	_, err := query.Compile(
		nil, dal.NewKeyBuilder(), accountHasAssetFilter("USD", 2),
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
		nil, nil, info, registry, resolverZero, nil, nil, 0)
	require.Error(t, err, "compiler must refuse when CurrentVersion=0")

	var building *domain.ErrIndexBuilding
	require.ErrorAs(t, err, &building,
		"has-asset compile must return ErrIndexBuilding when local IndexVersionState has CurrentVersion=0 (got %T: %v)", err, err)
}

// TestCompile_AccountHasAsset_PrefixScan seeds the account-by-asset index
// so accounts:alice & accounts:bob touched USD/2 and accounts:carol touched
// EUR/2, then compiles a has-asset(USD,2) condition with the index READY and
// asserts the iterator yields exactly {accounts:alice, accounts:bob}.
func TestCompile_AccountHasAsset_PrefixScan(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"

	// The account-by-asset index lives in the read store, whose Pebble
	// instance is configured with ReadStoreComparer — SeekPrefixGE in the
	// prefix iterator depends on that comparer's ledger-scoped Split.
	logger := logging.FromContext(logging.TestingContext())
	store, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	for _, seed := range []struct {
		account   string
		assetBase string
		precision uint8
	}{
		{"accounts:alice", "USD", 2},
		{"accounts:bob", "USD", 2},
		{"accounts:carol", "EUR", 2},
	} {
		key := readstore.AccountByAssetKey(kb, ledgerName, seed.assetBase, seed.precision, seed.account)
		require.NoError(t, batch.SetBytes(key, nil))
	}
	require.NoError(t, batch.Commit())

	reader := store.DB()

	assetID := indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)
	info := &commonpb.LedgerInfo{Name: ledgerName}
	registry := staticIndexLookup{
		indexes.KeyFor(ledgerName, assetID): {Ledger: ledgerName, Id: assetID},
	}
	resolverReady := func(string) (uint32, bool, error) { return 1, true, nil }

	iter, err := query.Compile(
		reader, dal.NewKeyBuilder(), accountHasAssetFilter("USD", 2),
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
		nil, nil, info, registry, resolverReady, nil, reader, 0)
	require.NoError(t, err)

	t.Cleanup(iter.Close)

	var got []string
	for iter.Next() {
		got = append(got, string(iter.Current()))
	}
	require.NoError(t, iter.Err())

	require.Equal(t, []string{"accounts:alice", "accounts:bob"}, got)
}

// TestCompile_AccountHasAsset_PinExcludesLaterFirstTouch pins the stamp gate.
// Each account-by-asset row carries its FIRST touch's fold sequence, and the
// aligned index snapshot legitimately holds touches folded past the main
// handle; a pinned scan must exclude exactly those. A row at or below the pin
// keeps serving — including the purged-account case, since the gate reads the
// stamp and never main-store existence.
func TestCompile_AccountHasAsset_PinExcludesLaterFirstTouch(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"

	logger := logging.FromContext(logging.TestingContext())
	store, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	for _, seed := range []struct {
		account string
		seq     uint64
	}{
		{"accounts:early", 5},
		{"accounts:late", 9},
	} {
		key := readstore.AccountByAssetKey(kb, ledgerName, "USD", 2, seed.account)
		stamp := make([]byte, 8)
		binary.BigEndian.PutUint64(stamp, seed.seq)
		require.NoError(t, batch.SetBytes(key, stamp))
	}
	require.NoError(t, batch.Commit())

	reader := store.DB()

	assetID := indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)
	info := &commonpb.LedgerInfo{Name: ledgerName}
	registry := staticIndexLookup{
		indexes.KeyFor(ledgerName, assetID): {Ledger: ledgerName, Id: assetID},
	}
	resolverReady := func(string) (uint32, bool, error) { return 1, true, nil }

	scan := func(pin uint64) []string {
		iter, cErr := query.Compile(
			reader, dal.NewKeyBuilder(), accountHasAssetFilter("USD", 2),
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
			nil, nil, info, registry, resolverReady, nil, reader, pin)
		require.NoError(t, cErr)

		defer iter.Close()

		var got []string
		for iter.Next() {
			got = append(got, string(iter.Current()))
		}
		require.NoError(t, iter.Err())

		return got
	}

	require.Equal(t, []string{"accounts:early"}, scan(7),
		"a first touch folded past the pin must be invisible to the pinned read")
	require.Equal(t, []string{"accounts:early", "accounts:late"}, scan(9),
		"a pin covering both stamps serves both members")
}

// TestCompile_RevertedAt_PinExcludesLaterRevert is the reverted_at twin of the
// has-asset stamp test. The rvat row is written by the REVERT's fold — after
// the transaction's creation — so the TRANSACTIONS existence trim proves
// nothing about it at a pin; only the row's stamp can exclude a revert folded
// past the main handle.
func TestCompile_RevertedAt_PinExcludesLaterRevert(t *testing.T) {
	t.Parallel()

	const ledgerName = "ledger1"

	logger := logging.FromContext(logging.TestingContext())
	store, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)

	t.Cleanup(func() { _ = store.Close() })

	kb := dal.NewKeyBuilder()
	batch := store.NewBatch()
	for _, seed := range []struct {
		txID uint64
		ts   uint64
		seq  uint64
	}{
		{7, 1_000, 5},
		{8, 2_000, 9},
	} {
		key := readstore.TransactionRevertedAtKey(kb, ledgerName, seed.ts, seed.txID)
		stamp := make([]byte, 8)
		binary.BigEndian.PutUint64(stamp, seed.seq)
		require.NoError(t, batch.SetBytes(key, stamp))
	}
	require.NoError(t, batch.Commit())

	reader := store.DB()

	rvatID := indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT)
	info := &commonpb.LedgerInfo{Name: ledgerName}
	registry := staticIndexLookup{
		indexes.KeyFor(ledgerName, rvatID): {Ledger: ledgerName, Id: rvatID},
	}
	resolverReady := func(string) (uint32, bool, error) { return 1, true, nil }

	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{
		BuiltinUint: &commonpb.BuiltinUintCondition{
			Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT,
			Cond:  &commonpb.UintCondition{},
		},
	}}

	scan := func(pin uint64) []uint64 {
		iter, cErr := query.Compile(
			reader, dal.NewKeyBuilder(), filter,
			commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, ledgerName,
			nil, nil, info, registry, resolverReady, nil, reader, pin)
		require.NoError(t, cErr)

		defer iter.Close()

		var got []uint64
		for iter.Next() {
			got = append(got, binary.BigEndian.Uint64(iter.Current()))
		}
		require.NoError(t, iter.Err())

		return got
	}

	require.Equal(t, []uint64{7}, scan(7),
		"a revert folded past the pin must be invisible to the pinned read")
	require.Equal(t, []uint64{7, 8}, scan(9),
		"a pin covering both stamps serves both reverts")
}
