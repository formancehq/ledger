package query_test

import (
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
	resolverZero := func(string) (uint32, error) { return 0, nil }

	_, err := query.Compile(
		nil, dal.NewKeyBuilder(), accountHasAssetFilter("USD", 2),
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
		nil, nil, info, registry, resolverZero, nil, nil)
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
	resolverReady := func(string) (uint32, error) { return 1, nil }

	iter, err := query.Compile(
		reader, dal.NewKeyBuilder(), accountHasAssetFilter("USD", 2),
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerName,
		nil, nil, info, registry, resolverReady, nil, reader)
	require.NoError(t, err)

	t.Cleanup(iter.Close)

	var got []string
	for iter.Next() {
		got = append(got, string(iter.Current()))
	}
	require.NoError(t, iter.Err())

	require.Equal(t, []string{"accounts:alice", "accounts:bob"}, got)
}
