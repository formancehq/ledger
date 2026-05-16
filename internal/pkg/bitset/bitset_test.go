package bitset

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()

	bs := New(0)
	require.NotNil(t, bs)
	require.Equal(t, uint64(1), bs.WordCount())

	bs = New(63)
	require.Equal(t, uint64(1), bs.WordCount())

	bs = New(64)
	require.Equal(t, uint64(2), bs.WordCount())

	bs = New(127)
	require.Equal(t, uint64(2), bs.WordCount())

	bs = New(128)
	require.Equal(t, uint64(3), bs.WordCount())
}

func TestSetAndTest(t *testing.T) {
	t.Parallel()

	bs := New(255)

	// Initially all bits should be unset
	for i := range uint64(256) {
		require.False(t, bs.Test(i), "bit %d should be unset initially", i)
	}

	// Set specific bits
	bs.Set(0)
	bs.Set(1)
	bs.Set(63)
	bs.Set(64)
	bs.Set(127)
	bs.Set(255)

	require.True(t, bs.Test(0))
	require.True(t, bs.Test(1))
	require.False(t, bs.Test(2))
	require.True(t, bs.Test(63))
	require.True(t, bs.Test(64))
	require.False(t, bs.Test(65))
	require.True(t, bs.Test(127))
	require.False(t, bs.Test(128))
	require.True(t, bs.Test(255))
}

func TestSetReturnsWordIndex(t *testing.T) {
	t.Parallel()

	bs := New(255)

	require.Equal(t, uint64(0), bs.Set(0))
	require.Equal(t, uint64(0), bs.Set(63))
	require.Equal(t, uint64(1), bs.Set(64))
	require.Equal(t, uint64(1), bs.Set(127))
	require.Equal(t, uint64(2), bs.Set(128))
	require.Equal(t, uint64(3), bs.Set(255))
}

func TestTestOutOfRange(t *testing.T) {
	t.Parallel()

	bs := New(63)
	require.False(t, bs.Test(64))
	require.False(t, bs.Test(1000))
}

func TestGrow(t *testing.T) {
	t.Parallel()

	bs := New(0)
	require.Equal(t, uint64(1), bs.WordCount())

	// Setting a bit beyond current capacity should grow
	bs.Set(200)
	require.True(t, bs.Test(200))
	require.True(t, bs.WordCount() >= 4)
}

func TestWord(t *testing.T) {
	t.Parallel()

	bs := New(127)

	bs.Set(0)
	bs.Set(1)
	require.Equal(t, uint64(3), bs.Word(0)) // bits 0 and 1 set = 0b11 = 3

	bs.Set(64)
	require.Equal(t, uint64(1), bs.Word(1)) // bit 0 of word 1 set = 1

	// Out of range word returns 0
	require.Equal(t, uint64(0), bs.Word(100))
}

func TestSetWord(t *testing.T) {
	t.Parallel()

	bs := New(127)

	bs.SetWord(0, 0xFF)
	require.Equal(t, uint64(0xFF), bs.Word(0))

	// Verify individual bits
	for i := range uint64(8) {
		require.True(t, bs.Test(i), "bit %d should be set", i)
	}
	require.False(t, bs.Test(8))

	// SetWord beyond current size should grow
	bs.SetWord(10, 42)
	require.Equal(t, uint64(42), bs.Word(10))
}

func TestClear(t *testing.T) {
	t.Parallel()

	bs := New(127)
	bs.Set(0)
	bs.Set(63)
	bs.Set(64)
	bs.Set(127)

	bs.Clear()

	for i := range uint64(128) {
		require.False(t, bs.Test(i), "bit %d should be unset after clear", i)
	}

	// WordCount should remain the same
	require.Equal(t, uint64(2), bs.WordCount())
}

func TestWords(t *testing.T) {
	t.Parallel()

	bs := New(63)
	bs.Set(0)
	bs.Set(63)

	words := bs.Words()
	require.Len(t, words, 1)
	require.Equal(t, uint64(1)|(uint64(1)<<63), words[0])
}

func TestMarshalWord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value uint64
	}{
		{"zero", 0},
		{"one", 1},
		{"max_uint64", ^uint64(0)},
		{"pattern", 0xDEADBEEFCAFEBABE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			buf := MarshalWord(tt.value)
			require.Len(t, buf, 8)
			require.Equal(t, tt.value, binary.LittleEndian.Uint64(buf))
		})
	}
}
