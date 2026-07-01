package readstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestUint64CursorRoundTrip(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	c := Uint64Cursor{key: []byte("test-cursor-key")}

	got, err := c.Read(s.db)
	require.NoError(t, err)
	require.Zero(t, got, "missing key reads as 0")

	batch := s.NewBatch()
	require.NoError(t, c.Write(batch, 42))
	require.NoError(t, batch.Commit())

	got, err = c.Read(s.db)
	require.NoError(t, err)
	require.Equal(t, uint64(42), got)
}

func TestUint64CursorCorruptLength(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	c := Uint64Cursor{key: []byte("corrupt-cursor-key")}

	// The read store opens Pebble with DisableWAL, so writes must use NoSync;
	// pebble.Sync would return "WAL disabled". NoSync is the same option the
	// store's own direct-DB writes (DeleteBackfillProgress) use.
	require.NoError(t, s.db.Set(c.key, []byte{1, 2, 3}, pebble.NoSync))

	_, err := c.Read(s.db)
	require.Error(t, err, "a non-8-byte value must be a hard error, not silently 0")
}
