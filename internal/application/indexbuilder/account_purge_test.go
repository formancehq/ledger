package indexbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestPurgeCurrentAccountIndexesReconcilesSameBatchMembership(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	batch := b.seedActiveBatch(t)
	b.seenAcctAsset = make(map[string]struct{})

	const ledger, account = "ledger", "hold:1"
	b.wb.SetEventSequence(1)
	require.NoError(t, b.writeAccountByAssetDedup(b.kb, ledger, account, "USD", 2))
	b.wb.SetEventSequence(2)
	require.NoError(t, b.purgeCurrentAccountIndexes(acctAssetConfig(), ledger, account))
	require.Empty(t, b.seenAcctAsset, "purge must invalidate in-batch dedup state")

	// A re-fund later in the same indexer batch must queue a Put after the
	// purge Delete and recreate current membership.
	b.wb.SetEventSequence(3)
	require.NoError(t, b.writeAccountByAssetDedup(b.kb, ledger, account, "USD", 2))
	require.NoError(t, batch.Commit())
	b.wb.Reset()

	key := readstore.AccountByAssetKey(dal.NewKeyBuilder(), ledger, "USD", 2, account)
	value, closer, err := b.readStore.DB().Get(key)
	require.NoError(t, err)
	require.NotEmpty(t, value)
	require.NoError(t, closer.Close())
}

func TestPurgeCurrentAccountIndexesRecreatesCommittedMembershipAfterSameBatchRefund(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	const ledger, account = "ledger", "hold:1"
	key := readstore.AccountByAssetKey(dal.NewKeyBuilder(), ledger, "USD", 2, account)

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.Set(key, []byte{1}, nil))
	require.NoError(t, seed.Commit())

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	b.wb.SetEventSequence(2)
	require.NoError(t, b.purgeCurrentAccountIndexes(acctAssetConfig(), ledger, account))

	// Pebble still exposes the committed row until this batch commits. The
	// refund must nevertheless enqueue a Put after the pending Delete.
	b.wb.SetEventSequence(3)
	require.NoError(t, b.writeAccountByAssetDedup(b.kb, ledger, account, "USD", 2))
	require.NoError(t, batch.Commit())
	b.wb.Reset()

	value, closer, err := b.readStore.DB().Get(key)
	require.NoError(t, err)
	require.NotEmpty(t, value)
	require.NoError(t, closer.Close())
}

func TestMarkLedgerDeletedInBatchPreservesOtherLedgerMemberships(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.seenAcctAsset = make(map[string]struct{})
	b.deletedThisBatch = make(map[string]struct{})
	ledgerAKey := readstore.AccountByAssetKey(dal.NewKeyBuilder(), "ledger-a", "USD", 2, "hold:1")
	ledgerBKey := readstore.AccountByAssetKey(dal.NewKeyBuilder(), "ledger-b", "USD", 2, "hold:2")
	b.seenAcctAsset[string(ledgerAKey)] = struct{}{}
	b.seenAcctAsset[string(ledgerBKey)] = struct{}{}

	b.markLedgerDeletedInBatch("ledger-b")

	require.Contains(t, b.seenAcctAsset, string(ledgerAKey))
	require.NotContains(t, b.seenAcctAsset, string(ledgerBKey))
}
