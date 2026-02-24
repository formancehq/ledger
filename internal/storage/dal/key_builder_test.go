package dal

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyBuilder_PutUInt64(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()
	key := kb.PutUInt64(42).Build()

	require.Len(t, key, 8)
	require.Equal(t, uint64(42), binary.BigEndian.Uint64(key))
}

func TestKeyBuilder_PutUInt32(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()
	key := kb.PutUInt32(100).Build()

	require.Len(t, key, 4)
	require.Equal(t, uint32(100), binary.BigEndian.Uint32(key))
}

func TestKeyBuilder_PutString(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()
	key := kb.PutString("hello").Build()

	require.Equal(t, []byte("hello"), key)
}

func TestKeyBuilder_PutByte(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()
	key := kb.PutByte(0xFF).Build()

	require.Equal(t, []byte{0xFF}, key)
}

func TestKeyBuilder_PutBytes(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()
	key := kb.PutBytes([]byte{1, 2, 3}).Build()

	require.Equal(t, []byte{1, 2, 3}, key)
}

func TestKeyBuilder_PutLedgerPrefix(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()
	key := kb.PutString("ledger-5").PutByte(0x00).Build()

	require.Equal(t, append([]byte("ledger-5"), 0x00), key)
}

func TestKeyBuilder_Chaining(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()
	key := kb.PutUInt32(1).PutByte(0x00).PutString("test").Build()

	require.Len(t, key, 4+1+4)
	require.Equal(t, uint32(1), binary.BigEndian.Uint32(key[:4]))
	require.Equal(t, byte(0x00), key[4])
	require.Equal(t, "test", string(key[5:]))
}

func TestKeyBuilder_Snapshot(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()
	kb.PutUInt32(1).PutByte(0x00)
	snapshot := kb.Snapshot()

	// Continue building
	key := kb.PutString("more").Build()

	// Snapshot should be the state before "more" was added
	require.Len(t, snapshot, 5)
	require.Len(t, key, 9)
}

func TestKeyBuilder_Reset(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()
	kb.PutUInt32(1).PutString("first")
	kb.Reset()

	key := kb.PutString("second").Build()
	require.Equal(t, []byte("second"), key)
}

func TestKeyBuilder_BuildResetsForReuse(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()

	key1 := kb.PutUInt32(1).Build()
	key2 := kb.PutUInt32(2).Build()

	require.Equal(t, uint32(1), binary.BigEndian.Uint32(key1))
	require.Equal(t, uint32(2), binary.BigEndian.Uint32(key2))
	// Keys should be independent (Build clones)
	require.Len(t, key1, 4)
	require.Len(t, key2, 4)
}

func TestKeyBuilder_KeyOrdering(t *testing.T) {
	t.Parallel()

	kb := NewKeyBuilder()

	// Keys with same prefix but different suffixes should sort correctly
	key1 := kb.PutUInt32(1).PutUInt64(10).Build()
	key2 := kb.PutUInt32(1).PutUInt64(20).Build()
	key3 := kb.PutUInt32(2).PutUInt64(1).Build()

	require.True(t, bytes.Compare(key1, key2) < 0, "key1 should sort before key2")
	require.True(t, bytes.Compare(key2, key3) < 0, "key2 should sort before key3")
}
