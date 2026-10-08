package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestKeyStoreDelete_Gen0Only exercises the well-formed flow: the entry is in
// Gen0 at Delete time, Del tombstones it in place, cache equals disk for the
// same applied index (invariant #1).
func TestKeyStoreDelete_Gen0Only(t *testing.T) {
	t.Parallel()

	c, err := New(10, nil)
	require.NoError(t, err)

	ac := c.LedgerMetadata
	ks := attributes.NewKeyStore[domain.LedgerMetadataKey, *commonpb.MetadataValue](ac)

	canonical := domain.LedgerMetadataKey{LedgerName: "l", Key: "k"}.Bytes()

	_, idWithTag, err := ks.Put(canonical, commonpb.NewStringValue("v"))
	require.NoError(t, err)

	id := idWithTag.ID
	tag := idWithTag.Tag

	_, _, err = ks.Delete(canonical)
	require.NoError(t, err)

	gen0Entry, ok := ac.Gen0().Get(id)
	require.True(t, ok, "Gen0 must hold a tombstone after delete")
	assert.True(t, gen0Entry.Deleted)
	assert.Equal(t, tag, gen0Entry.Tag)

	// KeyStore.Get filters tombstones — a read of the deleted key surfaces
	// as ErrNotFound.
	_, _, err = ks.Get(canonical)
	require.ErrorIs(t, err, domain.ErrNotFound)
}

// TestKeyStoreDelete_LazyPromoteFromGen1: after a rotation the live entry
// sits in Gen1 only. AttributeCache.Del fabricates a Gen0 tombstone from
// the Gen1 entry's tag; Gen1's live row is intentionally left untouched
// (shadowed by the Gen0 tombstone on every read, purged on the next
// rotation). The on-disk writeCacheTombstone writes a single row to the
// current gen0 byte, so cache stays equal to disk.
func TestKeyStoreDelete_LazyPromoteFromGen1(t *testing.T) {
	t.Parallel()

	c, err := New(10, nil)
	require.NoError(t, err)

	ac := c.LedgerMetadata
	ks := attributes.NewKeyStore[domain.LedgerMetadataKey, *commonpb.MetadataValue](ac)

	canonical := domain.LedgerMetadataKey{LedgerName: "l", Key: "k"}.Bytes()

	_, idWithTag, err := ks.Put(canonical, commonpb.NewStringValue("v"))
	require.NoError(t, err)

	id := idWithTag.ID
	tag := idWithTag.Tag

	// Rotate: the live entry moves Gen0 -> Gen1.
	ac.Rotate()

	// Del promotes the tombstone into Gen0 lazily — no separate MirrorTouch
	// pass is needed. Gen1 keeps its live row.
	_, _, err = ks.Delete(canonical)
	require.NoError(t, err)

	gen0Entry, ok := ac.Gen0().Get(id)
	require.True(t, ok, "Gen0 must hold a fabricated tombstone after lazy promote")
	assert.True(t, gen0Entry.Deleted)
	assert.Equal(t, tag, gen0Entry.Tag, "tombstone borrows Gen1's tag")

	// Gen1 stays live — shadowed by the Gen0 tombstone on read, purged on
	// the next rotation.
	gen1Entry, ok := ac.Gen1().Get(id)
	require.True(t, ok, "Gen1 must keep its pre-rotation row")
	assert.False(t, gen1Entry.Deleted, "Gen1 entry is not tombstoned by Del")

	// KeyStore.Get surfaces the Gen0 tombstone as ErrNotFound.
	_, _, err = ks.Get(canonical)
	require.ErrorIs(t, err, domain.ErrNotFound)
}

// TestKeyStoreDelete_AbsentReturnsNotFound: Del on a key present in
// neither generation surfaces domain.ErrNotFound. DerivedKeyStore.Merge
// treats this as a soft skip; under proper admission every Delete is
// preceded by a Get that would surface the same ErrNotFound before Del
// is reached.
func TestKeyStoreDelete_AbsentReturnsNotFound(t *testing.T) {
	t.Parallel()

	c, err := New(10, nil)
	require.NoError(t, err)

	ac := c.LedgerMetadata
	ks := attributes.NewKeyStore[domain.LedgerMetadataKey, *commonpb.MetadataValue](ac)

	canonical := domain.LedgerMetadataKey{LedgerName: "l", Key: "k"}.Bytes()

	_, _, err = ks.Delete(canonical)
	require.ErrorIs(t, err, domain.ErrNotFound)

	// No fabricated tombstone.
	id, _ := attributes.MakeKey(canonical)
	_, ok := ac.Gen0().Get(id)
	assert.False(t, ok)
	_, ok = ac.Gen1().Get(id)
	assert.False(t, ok)
}
