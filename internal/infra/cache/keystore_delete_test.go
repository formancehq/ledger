package cache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestKeyStoreDelete_TombstonesAllGenerations is the EN-1242 regression: a
// delete on a key whose live entry sits in Gen1 (post-rotation) must tombstone
// BOTH generations, matching the dual-gen tombstone the FSM writes to the 0xFF
// zone in the same batch. A Gen0-only write leaves Gen1 holding the live entry
// — hidden from Get (which reads Gen0 first) yet diverging from disk until a
// restart rehydrates Gen1 as a tombstone.
func TestKeyStoreDelete_TombstonesAllGenerations(t *testing.T) {
	t.Parallel()

	c, err := New(10, nil)
	require.NoError(t, err)

	ac := c.LedgerMetadata
	ks := attributes.NewKeyStore[domain.LedgerMetadataKey, *commonpb.MetadataValue](ac)

	canonical := domain.LedgerMetadataKey{LedgerName: "l", Key: "k"}.Bytes()

	_, idWithTag, err := ks.Put(canonical, commonpb.NewStringValue("v"))
	require.NoError(t, err)
	id := idWithTag.ID

	// Rotate: the live entry moves Gen0 -> Gen1, Gen0 is fresh and empty.
	ac.Rotate()

	_, gen0Has := ac.Gen0().Get(id)
	require.False(t, gen0Has, "precondition: Gen0 empty after rotation")
	live, gen1Has := ac.Gen1().Get(id)
	require.True(t, gen1Has)
	require.False(t, live.Deleted, "precondition: live entry in Gen1")

	_, _, err = ks.Delete(canonical)
	require.NoError(t, err)

	gen0Entry, ok := ac.Gen0().Get(id)
	require.True(t, ok, "Gen0 must hold a tombstone after delete")
	require.True(t, gen0Entry.Deleted)

	gen1Entry, ok := ac.Gen1().Get(id)
	require.True(t, ok)
	require.True(t, gen1Entry.Deleted, "Gen1 live entry must become a tombstone")

	_, _, err = ks.Get(canonical)
	require.ErrorIs(t, err, domain.ErrNotFound)
}
