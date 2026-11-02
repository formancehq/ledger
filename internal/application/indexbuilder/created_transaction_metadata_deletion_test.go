package indexbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// TestIndexCreatedTransaction_DeletesAccountMetadata pins that a key a
// Numscript transaction deleted leaves the account-metadata index exactly as
// a DeletedMetadata log does, including the pending version of a rewrite.
func TestIndexCreatedTransaction_DeletesAccountMetadata(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	b.seedBatchSchema(t)

	const (
		ledger  = "test"
		account = "acct-404"
		key     = "score"
	)
	entityID := []byte(account)
	canonical := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))

	b.putVersionState(ledger, canonical, readstore.IndexVersionState{
		CurrentVersion: 1,
		PendingVersion: 2,
	})

	cfg := newLedgerIndexConfig()
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewIntValue(42))

	v1Rmap := cloneBytes(readstore.AccountReverseMapKeyV(b.kb, ledger, account, key, 1))
	v2Rmap := cloneBytes(readstore.AccountReverseMapKeyV(b.kb, ledger, account, key, 2))

	seed := b.readStore.NewBatch()
	require.NoError(t, seed.SetBytes(v1Rmap, encoded))
	require.NoError(t, seed.SetBytes(v2Rmap, encoded))
	require.NoError(t, seed.Commit())
	seedMetadataEvent(t, b, ledger, readstore.NamespaceAccount, key, 1, encoded, entityID, 1, readstore.MetadataEventAdd)
	seedMetadataEvent(t, b, ledger, readstore.NamespaceAccount, key, 2, encoded, entityID, 1, readstore.MetadataEventAdd)

	ct := &commonpb.CreatedTransaction{
		Transaction: &commonpb.Transaction{Id: 1},
		DeletedAccountMetadata: map[string]*commonpb.MetadataKeys{
			account: {Keys: []string{key}},
		},
	}

	batch := b.readStore.NewBatch()
	b.wb.Init(batch)
	b.wb.SetEventSequence(2)
	require.NoError(t, b.indexCreatedTransaction(b.kb, cfg, ledger, ct, nil))
	require.NoError(t, b.wb.Flush())

	requireMetadataDead(t, b, ledger, readstore.NamespaceAccount, key, 1, encoded, entityID)
	requireMetadataDead(t, b, ledger, readstore.NamespaceAccount, key, 2, encoded, entityID)
	assertReadStoreMissing(t, b, v1Rmap)
	assertReadStoreMissing(t, b, v2Rmap)
}
