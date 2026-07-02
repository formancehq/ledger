package indexbuilder

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBootInit_PropagatesError pins that the extracted boot prologue surfaces a
// read failure (via initIndexConfig / LastIndexedSequence / NewDirectReadHandle)
// so loop's RetryWithBackoff wrapper can retry instead of silently proceeding.
func TestBootInit_PropagatesError(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)
	require.NoError(t, b.pebbleStore.Close())

	_, _, err := b.bootInit(context.Background())
	require.Error(t, err)
}

// TestBootInit_Success returns the persisted cursor and pebbleLast on a healthy
// store (empty store: cursor 0, pebbleLast 0).
func TestBootInit_Success(t *testing.T) {
	t.Parallel()

	b := newTestBuilderWithStore(t)

	cursor, pebbleLast, err := b.bootInit(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(0), cursor)
	require.Equal(t, uint64(0), pebbleLast)
}
