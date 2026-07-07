package dal

import (
	"io"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestProtoCursor_Basic(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write proto messages under a prefix. NullValue is the simplest still-a-
	// message type in commonpb after Timestamp was inlined into a scalar.
	batch := s.OpenWriteSession()

	nv1 := &commonpb.NullValue{Original: "a"}
	nv2 := &commonpb.NullValue{Original: "b"}
	nv3 := &commonpb.NullValue{Original: "c"}

	kb := NewKeyBuilder()
	key1 := kb.PutByte(0xAA).PutUint64(1).Build()
	key2 := kb.PutByte(0xAA).PutUint64(2).Build()
	key3 := kb.PutByte(0xAA).PutUint64(3).Build()

	data1, err := nv1.MarshalVT()
	require.NoError(t, err)
	data2, err := nv2.MarshalVT()
	require.NoError(t, err)
	data3, err := nv3.MarshalVT()
	require.NoError(t, err)

	require.NoError(t, batch.SetBytes(key1, data1))
	require.NoError(t, batch.SetBytes(key2, data2))
	require.NoError(t, batch.SetBytes(key3, data3))
	require.NoError(t, batch.Commit())

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0xAA},
		UpperBound: []byte{0xAB},
	})
	require.NoError(t, err)

	cursor := NewProtoCursor[*commonpb.NullValue](iter)

	defer func() { _ = cursor.Close() }()

	got1, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, "a", got1.GetOriginal())

	got2, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, "b", got2.GetOriginal())

	got3, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, "c", got3.GetOriginal())

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestProtoCursor_Empty(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0xBB},
		UpperBound: []byte{0xBC},
	})
	require.NoError(t, err)

	cursor := NewProtoCursor[*commonpb.NullValue](iter)

	defer func() { _ = cursor.Close() }()

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestProtoCursor_CloseNilIter(t *testing.T) {
	t.Parallel()

	cursor := &ProtoCursor[*commonpb.NullValue]{}
	require.NoError(t, cursor.Close())
}

func TestProtoCursor_MultipleCallsAfterEOF(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	batch := s.OpenWriteSession()
	nv := &commonpb.NullValue{Original: "sole"}
	data, err := nv.MarshalVT()
	require.NoError(t, err)

	kb := NewKeyBuilder()
	key := kb.PutByte(0xCC).PutUint64(1).Build()
	require.NoError(t, batch.SetBytes(key, data))
	require.NoError(t, batch.Commit())

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0xCC},
		UpperBound: []byte{0xCD},
	})
	require.NoError(t, err)

	cursor := NewProtoCursor[*commonpb.NullValue](iter)

	defer func() { _ = cursor.Close() }()

	got, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, "sole", got.GetOriginal())

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}
