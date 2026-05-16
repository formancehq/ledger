package vtmarshal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// fakeMsg implements VTMarshaler for testing.
type fakeMsg struct {
	data []byte
}

func (f *fakeMsg) SizeVT() int { return len(f.data) }

func (f *fakeMsg) MarshalToVT(buf []byte) (int, error) {
	return copy(buf, f.data), nil
}

func TestMarshalCopy(t *testing.T) {
	t.Parallel()

	data := []byte("hello world")
	msg := &fakeMsg{data: data}

	out, err := MarshalCopy(msg)
	require.NoError(t, err)
	require.Equal(t, data, out)

	// Verify the returned slice is a copy, not the same underlying array
	out[0] = 'X'
	require.Equal(t, byte('h'), data[0])
}

func TestMarshalCopySlack(t *testing.T) {
	t.Parallel()

	data := []byte("test")
	msg := &fakeMsg{data: data}

	out, err := MarshalCopy(msg)
	require.NoError(t, err)
	require.Equal(t, len(data), len(out))
	require.Equal(t, len(data)+marshalSlack, cap(out))
}

func TestMarshalCopyEmpty(t *testing.T) {
	t.Parallel()

	msg := &fakeMsg{data: nil}

	out, err := MarshalCopy(msg)
	require.NoError(t, err)
	require.Empty(t, out)
}

func TestMarshalCopyLargeMessage(t *testing.T) {
	t.Parallel()

	// Larger than the default pool buffer (4096)
	data := make([]byte, 8192)
	for i := range data {
		data[i] = byte(i % 256)
	}

	msg := &fakeMsg{data: data}

	out, err := MarshalCopy(msg)
	require.NoError(t, err)
	require.Equal(t, data, out)
}

func TestMarshalCopyPoolReuse(t *testing.T) {
	t.Parallel()

	// Call multiple times to exercise pool get/put
	for i := range 100 {
		data := []byte{byte(i), byte(i + 1), byte(i + 2)}
		msg := &fakeMsg{data: data}

		out, err := MarshalCopy(msg)
		require.NoError(t, err)
		require.Equal(t, data, out)
	}
}
