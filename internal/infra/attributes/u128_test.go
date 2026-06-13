package attributes

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewU128(t *testing.T) {
	t.Parallel()

	t.Run("zero", func(t *testing.T) {
		t.Parallel()

		u := NewU128(0, 0)
		require.Equal(t, uint64(0), u.Hi())
		require.Equal(t, uint64(0), u.Lo())
	})

	t.Run("high only", func(t *testing.T) {
		t.Parallel()

		u := NewU128(0xDEADBEEF, 0)
		require.Equal(t, uint64(0xDEADBEEF), u.Hi())
		require.Equal(t, uint64(0), u.Lo())
	})

	t.Run("low only", func(t *testing.T) {
		t.Parallel()

		u := NewU128(0, 0xCAFEBABE)
		require.Equal(t, uint64(0), u.Hi())
		require.Equal(t, uint64(0xCAFEBABE), u.Lo())
	})

	t.Run("both", func(t *testing.T) {
		t.Parallel()

		u := NewU128(0x1234567890ABCDEF, 0xFEDCBA0987654321)
		require.Equal(t, uint64(0x1234567890ABCDEF), u.Hi())
		require.Equal(t, uint64(0xFEDCBA0987654321), u.Lo())
	})

	t.Run("max values", func(t *testing.T) {
		t.Parallel()

		u := NewU128(^uint64(0), ^uint64(0))
		require.Equal(t, ^uint64(0), u.Hi())
		require.Equal(t, ^uint64(0), u.Lo())
	})
}

func TestU128FromBytes(t *testing.T) {
	t.Parallel()

	t.Run("exact 16 bytes", func(t *testing.T) {
		t.Parallel()

		b := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
		u := U128FromBytes(b)
		require.Equal(t, b, u.Bytes())
	})

	t.Run("shorter than 16 bytes is zero-padded", func(t *testing.T) {
		t.Parallel()

		b := []byte{0xAA, 0xBB}
		u := U128FromBytes(b)
		expected := U128{0xAA, 0xBB}
		require.Equal(t, expected, u)
	})

	t.Run("longer than 16 bytes is truncated", func(t *testing.T) {
		t.Parallel()

		b := make([]byte, 20)
		for i := range b {
			b[i] = byte(i + 1)
		}

		u := U128FromBytes(b)
		require.Equal(t, b[:16], u.Bytes())
	})

	t.Run("nil input", func(t *testing.T) {
		t.Parallel()

		u := U128FromBytes(nil)
		require.Equal(t, U128{}, u)
	})

	t.Run("empty input", func(t *testing.T) {
		t.Parallel()

		u := U128FromBytes([]byte{})
		require.Equal(t, U128{}, u)
	})
}

func TestU128Bytes(t *testing.T) {
	t.Parallel()

	u := NewU128(0x0102030405060708, 0x090A0B0C0D0E0F10)
	b := u.Bytes()
	require.Len(t, b, 16)

	expected := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10}
	require.Equal(t, expected, b)

	// Verify the returned slice is a copy (mutating it should not affect original)
	b[0] = 0xFF
	require.NotEqual(t, b[0], u[0])
}

func TestU128Hex(t *testing.T) {
	t.Parallel()

	t.Run("zero", func(t *testing.T) {
		t.Parallel()

		u := NewU128(0, 0)
		require.Equal(t, "00000000000000000000000000000000", u.Hex())
	})

	t.Run("non-zero", func(t *testing.T) {
		t.Parallel()

		u := NewU128(0x0102030405060708, 0x090A0B0C0D0E0F10)
		require.Equal(t, "0102030405060708090a0b0c0d0e0f10", u.Hex())
	})
}

func TestU128Equal(t *testing.T) {
	t.Parallel()

	t.Run("equal", func(t *testing.T) {
		t.Parallel()

		a := NewU128(42, 99)
		b := NewU128(42, 99)
		require.True(t, a.Equal(b))
	})

	t.Run("different hi", func(t *testing.T) {
		t.Parallel()

		a := NewU128(1, 99)
		b := NewU128(2, 99)
		require.False(t, a.Equal(b))
	})

	t.Run("different lo", func(t *testing.T) {
		t.Parallel()

		a := NewU128(42, 1)
		b := NewU128(42, 2)
		require.False(t, a.Equal(b))
	})

	t.Run("zero equal to zero", func(t *testing.T) {
		t.Parallel()

		a := U128{}
		b := U128{}
		require.True(t, a.Equal(b))
	})
}

func TestHashU128(t *testing.T) {
	t.Parallel()

	t.Run("deterministic", func(t *testing.T) {
		t.Parallel()

		input := []byte("platform:region-eu:merchant-42:wallets:main")
		h1 := HashU128(input)
		h2 := HashU128(input)
		require.Equal(t, h1, h2)
	})

	t.Run("different inputs produce different hashes", func(t *testing.T) {
		t.Parallel()

		h1 := HashU128([]byte("input-a"))
		h2 := HashU128([]byte("input-b"))
		require.False(t, h1.Equal(h2))
	})
}

func TestTag64(t *testing.T) {
	t.Parallel()

	t.Run("deterministic", func(t *testing.T) {
		t.Parallel()

		input := []byte("test-input")
		tag1 := Tag64(input)
		tag2 := Tag64(input)
		require.Equal(t, tag1, tag2)
	})

	t.Run("different inputs produce different tags", func(t *testing.T) {
		t.Parallel()

		tag1 := Tag64([]byte("input-x"))
		tag2 := Tag64([]byte("input-y"))
		require.NotEqual(t, tag1, tag2)
	})
}

func TestMakeKey(t *testing.T) {
	t.Parallel()

	t.Run("returns both id and tag", func(t *testing.T) {
		t.Parallel()

		input := []byte("test-key")
		id, tag := MakeKey(input)

		// Verify consistency with individual functions
		expectedID := HashU128(input)
		expectedTag := Tag64(input)

		require.Equal(t, expectedID, id)
		require.Equal(t, expectedTag, tag)
	})

	t.Run("id and tag are non-zero on real input", func(t *testing.T) {
		t.Parallel()

		input := []byte("check-non-zero")
		id, tag := MakeKey(input)
		require.NotEqual(t, U128{}, id)
		require.NotEqual(t, uint64(0), tag)
	})
}
