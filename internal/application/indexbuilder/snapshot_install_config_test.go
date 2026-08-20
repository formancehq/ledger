package indexbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// A follower caught up by a leader InstallSnapshot has its Pebble DB reopened
// with the leader's copy: node.processReady calls applier.SyncSnapshot, which
// enqueues RestoreCheckpoint. Everything holding state derived from the old DB
// has to be rebuilt across that swap, and NewNode wires a single callback for
// it — membership.OnSnapshotInstalled, which refreshes the peer cache.
//
// The builder reads the registry into indexConfig once, in bootInit, so a
// builder that finished booting before the swap keeps describing the pre-swap
// registry. Indexes dropped while the node was away stay live to it: it holds
// them in byCanonical, leaves their read-store rows unpurged, and reports no
// index to drop when the declaration is removed — which is what a cluster
// surfaces as an index outliving its field declaration.
func TestIndexConfigFollowsTheStoreAcrossASnapshotInstall(t *testing.T) {
	t.Parallel()

	const ledger = "restored-ledger"

	b := newTestBuilderWithStore(t)
	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k0")
	canonical := indexes.Canonical(id)
	indexKey := domain.IndexKey{LedgerName: ledger, Canonical: canonical}.Bytes()

	// Boot against a store holding the ledger and its index, the way a node
	// that indexed the field before it fell behind would.
	fsmBatch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, state.SaveLedger(fsmBatch, ledger, &commonpb.LedgerInfo{
		Name: ledger,
		MetadataSchema: &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"k0": {Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			},
		},
	}))
	_, err := b.attrs.Index.Set(fsmBatch, indexKey, &commonpb.Index{
		Ledger:                 ledger,
		Id:                     id,
		BuildStatus:            commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
		ForwardEncodingVersion: 1,
	})
	require.NoError(t, err)
	require.NoError(t, fsmBatch.Commit())

	require.NoError(t, b.initIndexConfig(context.Background()))
	require.Contains(t, b.ledgerConfig(ledger).byCanonical, canonical, "boot loads the index the store holds")

	// The leader's checkpoint replaces the store. This index was dropped while
	// the node was away, so the restored registry no longer carries it.
	swapBatch := b.pebbleStore.OpenWriteSession()
	require.NoError(t, b.attrs.Index.Delete(swapBatch, indexKey))
	require.NoError(t, swapBatch.Commit())

	// What the node signals on install, and what the loop does with it at its
	// next safe point.
	b.OnSnapshotInstalled()
	require.NoError(t, b.reloadConfigIfRequested(context.Background()))

	assert.NotContains(t, b.ledgerConfig(ledger).byCanonical, canonical,
		"the builder still holds an index the restored registry dropped: its rows are never purged, "+
			"and removing the declaration reports no index to drop")
}
