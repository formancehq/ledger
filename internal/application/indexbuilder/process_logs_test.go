package indexbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCloneBytes(t *testing.T) {
	t.Parallel()

	t.Run("normal slice", func(t *testing.T) {
		t.Parallel()

		original := []byte{1, 2, 3, 4, 5}
		cloned := cloneBytes(original)

		assert.Equal(t, original, cloned)
		// Verify it is a distinct allocation.
		original[0] = 99
		assert.NotEqual(t, original[0], cloned[0])
	})

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()

		original := []byte{}
		cloned := cloneBytes(original)

		assert.Equal(t, original, cloned)
		assert.Equal(t, 0, len(cloned))
	})

	t.Run("nil slice", func(t *testing.T) {
		t.Parallel()

		// nil slice treated as zero-length.
		cloned := cloneBytes(nil)
		assert.Equal(t, 0, len(cloned))
	})
}

// Metadata overwrites and deletes resolve the old value from the index's own
// reverse map (see reverseMapValue). Newly minted transaction metadata uses the
// separate insert-known-absent path and needs no old value.
