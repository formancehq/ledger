package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// EN-1522 gap A, one layer down — the Merge flush writes two rows per ledger
// update from the same ledgerUpdates slice: the SubAttrLedger attribute/cache
// row, keyed off the canonical attribute key, and the SubGlobLedgerInfo global
// row via SaveLedger. Keying the latter off the payload's mutable
// LedgerInfo.name would split the pair whenever the two disagree, and nothing
// would catch it: the checker's ledger passes re-derive the name from the
// payload, so a row filed under the wrong key reads as a different ledger.
func TestMergeKeysGlobalLedgerRowOffCanonicalKey(t *testing.T) {
	t.Parallel()

	buf, _, dataStore := newTestBuffer(t)

	const (
		envelope   = "envelope-ledger"
		divergent  = "divergent-projection"
		otherAsset = "USD"
	)

	// The row's identity is the envelope key; its payload carries a name that
	// disagrees with it.
	buf.Ledgers().Put(domain.LedgerKey{Name: envelope}, &commonpb.LedgerInfo{
		Id:   1,
		Name: divergent,
	})

	batch := dataStore.OpenWriteSession()
	require.NoError(t, buf.Merge(batch, nil))
	require.NoError(t, batch.Commit())

	ctx := context.Background()

	// The global row must be reachable under the identity it was written for.
	got, err := query.GetLedgerByName(ctx, dataStore, envelope)
	require.NoError(t, err)
	require.NotNil(t, got, "global ledger row must be keyed off the canonical attribute key")
	require.Equal(t, uint32(1), got.GetId())

	// And must NOT have been filed under the payload's divergent name, which
	// would strand it: GetLedgerByName is a point lookup, so the ledger would
	// answer to a name it was never created under.
	_, err = query.GetLedgerByName(ctx, dataStore, divergent)
	require.ErrorIs(t, err, domain.ErrNotFound, "no global row may be filed under the payload's divergent name")
}
