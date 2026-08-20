package plan

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
)

func TestCoverageInlineAndPromotion(t *testing.T) {
	t.Parallel()

	c := NewCoverage()
	for i := range inlineCoverageCapacity {
		c.Add(byte(i%2+1), fmt.Appendf(nil, "key-%d", i))
	}

	require.Nil(t, c.Attributes, "small coverage must stay out of the map representation")
	require.Equal(t, inlineCoverageCapacity, c.AttributeKeysCount())
	require.Equal(t, inlineCoverageCapacity/2, c.Count(1))
	require.True(t, c.Has(1, []byte("key-0")))

	// Duplicate insertion is idempotent and must not force promotion.
	c.Add(1, []byte("key-0"))
	require.Nil(t, c.Attributes)
	require.Equal(t, inlineCoverageCapacity, c.AttributeKeysCount())

	// The first distinct overflow entry promotes every inline row into the
	// historical grouped representation without losing identity.
	c.Add(3, []byte("overflow"))
	require.NotNil(t, c.Attributes)
	require.Empty(t, c.inline)
	require.Equal(t, inlineCoverageCapacity+1, c.AttributeKeysCount())
	require.True(t, c.Has(1, []byte("key-0")))
	require.True(t, c.Has(3, []byte("overflow")))
}

func TestCoverageInlineMergeDeduplicates(t *testing.T) {
	t.Parallel()

	dst := NewCoverage()
	src := NewCoverage()
	dst.Add(1, []byte("shared"))
	src.Add(1, []byte("shared"))
	src.Add(2, []byte("other"))

	dst.Merge(src)

	require.Nil(t, dst.Attributes)
	require.Equal(t, 2, dst.AttributeKeysCount())
	require.True(t, dst.Has(1, []byte("shared")))
	require.True(t, dst.Has(2, []byte("other")))
}

func TestCoverageInlineCollisionRecordsError(t *testing.T) {
	t.Parallel()

	id := attributes.NewU128(42, 42)
	c := NewCoverage()
	c.addHashed(1, id, CoverageEntry{Canonical: []byte("first"), Tag: 1})
	c.addHashed(1, id, CoverageEntry{Canonical: []byte("second"), Tag: 2})

	require.Error(t, c.Err())
	require.Equal(t, 1, c.AttributeKeysCount(), "colliding entry must not replace or duplicate the first")
}

// A genuine XXH3-128 collision between two distinct canonical keys is a
// ~2^-128 event that cannot be produced by hashing real inputs, so these
// tests inject the colliding shape directly (white-box) to exercise the
// production safety net: assert.Unreachable is a no-op in prod builds, so
// Add/Merge must ALSO record a returnable error that Build surfaces,
// instead of silently dropping the second key and letting the order reach
// apply without its preload seed (invariant #7).

func TestCoverageMergeCollisionRecordsError(t *testing.T) {
	t.Parallel()

	const attrCode = byte(1)
	id := attributes.NewU128(42, 42) // same 128-bit id on both sides

	dst := &Coverage{
		Attributes: map[byte]map[attributes.U128]CoverageEntry{
			attrCode: {id: {Canonical: []byte("first"), Tag: 1}},
		},
	}
	src := &Coverage{
		Attributes: map[byte]map[attributes.U128]CoverageEntry{
			attrCode: {id: {Canonical: []byte("second"), Tag: 2}}, // different tag
		},
	}

	require.NoError(t, dst.Err())

	dst.Merge(src)

	err := dst.Err()
	require.Error(t, err)
	var col *attributes.ErrCollisionDetected
	require.ErrorAs(t, err, &col)

	// The first entry is retained (not overwritten by the collider).
	require.Equal(t, uint64(1), dst.Attributes[attrCode][id].Tag)
}

func TestCoverageMergePropagatesSourceError(t *testing.T) {
	t.Parallel()

	src := &Coverage{collision: &attributes.ErrCollisionDetected{Bytes: []byte("x"), OriginalTag: 1, NewTag: 2}}
	dst := &Coverage{}

	dst.Merge(src)

	require.Error(t, dst.Err())
}

func TestBuildFailsOnCoverageCollision(t *testing.T) {
	t.Parallel()

	agg := &Coverage{collision: &attributes.ErrCollisionDetected{Bytes: []byte("x"), OriginalTag: 1, NewTag: 2}}

	// The collision check fires before any tracker/cache access, so a
	// zero-value Builder is enough.
	p := &Builder{}

	build, err := p.Build(agg, []WriteOperation{{Coverage: agg}})

	require.Error(t, err)
	var col *attributes.ErrCollisionDetected
	require.ErrorAs(t, err, &col)

	// A non-nil BuildResult must come back so the caller's error path can
	// safely call ReleaseLoaders (no loaders acquired yet -> no panic).
	require.NotNil(t, build)
	require.NotPanics(t, build.ReleaseLoaders)
}
