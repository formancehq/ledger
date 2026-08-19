package indexbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// registryHasEntry answers whether the persisted registry still holds an index
// when a field removal dropped nothing — the finding it feeds separates a
// registry that kept the entry from one that lost it, so a probe that always
// answered the same way would read as a confident diagnosis of the wrong half.
func TestRegistryHasEntry(t *testing.T) {
	t.Parallel()

	const ledger = "probe-ledger"

	b := newTestBuilderWithStore(t)
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0")
	canonical := indexes.Canonical(id)

	// Absent before anything is written.
	found, err := b.registryHasEntry(ledger, canonical)
	require.NoError(t, err)
	assert.False(t, found, "no entry has been written yet")

	fsmBatch := b.pebbleStore.OpenWriteSession()
	_, err = b.attrs.Index.Set(fsmBatch, domain.IndexKey{LedgerName: ledger, Canonical: canonical}.Bytes(), &commonpb.Index{
		Ledger:                 ledger,
		Id:                     id,
		BuildStatus:            commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
		ForwardEncodingVersion: 1,
	})
	require.NoError(t, err)
	require.NoError(t, fsmBatch.Commit())

	found, err = b.registryHasEntry(ledger, canonical)
	require.NoError(t, err)
	assert.True(t, found, "the entry is in the registry")

	// A different ledger and a different key must not match it.
	found, err = b.registryHasEntry("other-ledger", canonical)
	require.NoError(t, err)
	assert.False(t, found, "the entry belongs to another ledger")

	found, err = b.registryHasEntry(ledger, indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "k0")))
	require.NoError(t, err)
	assert.False(t, found, "a different target's index is a different entry")
}
