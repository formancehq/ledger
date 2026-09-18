package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestKeyStoreTombstone_Gen0Only exercises the well-formed flow: the entry is in
// Gen0 at Tombstone time, Tombstone writes it in place, cache equals disk for the
// same applied index (invariant #1).
func TestKeyStoreTombstone_Gen0Only(t *testing.T) {
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

	_, _, err = ks.Tombstone(canonical)
	require.NoError(t, err)

	gen0Entry, ok := ac.Gen0().Get(id)
	require.True(t, ok, "Gen0 must hold a tombstone")
	assert.True(t, gen0Entry.Deleted)
	assert.Equal(t, tag, gen0Entry.Tag)
	assert.Equal(t, CacheHit, ac.CheckCache(5, id),
		"a tombstone must remain a cache hit in the current generation")
	assert.Equal(t, CacheHit, ac.CheckCache(15, id),
		"a Gen0 tombstone must remain reachable after the predicted rotation")

	// KeyStore.Get filters tombstones — a read of the deleted key surfaces
	// as ErrNotFound.
	_, _, err = ks.Get(canonical)
	require.ErrorIs(t, err, domain.ErrNotFound)
}

// TestKeyStoreTombstone_LazyPromoteFromGen1: after a rotation the live entry
// sits in Gen1 only. KeyStore.Tombstone writes a Gen0 tombstone with
// the Gen1 entry's tag; Gen1's live row is intentionally left untouched
// (shadowed by the Gen0 tombstone on every read, purged on the next
// rotation). The on-disk writeCacheTombstone writes a single row to the
// current gen0 byte, so cache stays equal to disk.
func TestKeyStoreTombstone_LazyPromoteFromGen1(t *testing.T) {
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

	// Tombstone writes the tombstone into Gen0 lazily — no separate MirrorTouch
	// pass is needed. Gen1 keeps its live row.
	_, _, err = ks.Tombstone(canonical)
	require.NoError(t, err)

	gen0Entry, ok := ac.Gen0().Get(id)
	require.True(t, ok, "Gen0 must hold a fabricated tombstone after lazy promote")
	assert.True(t, gen0Entry.Deleted)
	assert.Equal(t, tag, gen0Entry.Tag, "tombstone borrows Gen1's tag")

	// Gen1 stays live — shadowed by the Gen0 tombstone on read, purged on
	// the next rotation.
	gen1Entry, ok := ac.Gen1().Get(id)
	require.True(t, ok, "Gen1 must keep its pre-rotation row")
	assert.False(t, gen1Entry.Deleted, "Gen1 entry is not tombstoned")

	// KeyStore.Get surfaces the Gen0 tombstone as ErrNotFound.
	_, _, err = ks.Get(canonical)
	require.ErrorIs(t, err, domain.ErrNotFound)
}

// TestKeyStoreTombstone_AbsentReturnsNotFound: Tombstone on a key present in
// neither generation surfaces domain.ErrNotFound. DerivedKeyStore.Merge
// treats this as a soft skip; under proper admission every tombstone is
// preceded by a Get that would surface the same ErrNotFound before Tombstone
// is reached.
func TestKeyStoreTombstone_AbsentReturnsNotFound(t *testing.T) {
	t.Parallel()

	c, err := New(10, nil)
	require.NoError(t, err)

	ac := c.LedgerMetadata
	ks := attributes.NewKeyStore[domain.LedgerMetadataKey, *commonpb.MetadataValue](ac)

	canonical := domain.LedgerMetadataKey{LedgerName: "l", Key: "k"}.Bytes()

	_, _, err = ks.Tombstone(canonical)
	require.ErrorIs(t, err, domain.ErrNotFound)

	// No fabricated tombstone.
	id, _ := attributes.MakeKey(canonical)
	_, ok := ac.Gen0().Get(id)
	assert.False(t, ok)
	_, ok = ac.Gen1().Get(id)
	assert.False(t, ok)
}
