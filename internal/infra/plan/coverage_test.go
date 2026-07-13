package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
)

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
