package dal

import (
	"io"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestProtoCursor_Basic(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write proto messages under a prefix
	batch := s.NewBatch()

	ts1 := &commonpb.Timestamp{Data: 1000}
	ts2 := &commonpb.Timestamp{Data: 2000}
	ts3 := &commonpb.Timestamp{Data: 3000}

	kb := NewKeyBuilder()
	key1 := kb.PutByte(0xAA).PutUInt64(1).Build()
	key2 := kb.PutByte(0xAA).PutUInt64(2).Build()
	key3 := kb.PutByte(0xAA).PutUInt64(3).Build()

	data1, err := proto.Marshal(ts1)
	require.NoError(t, err)
	data2, err := proto.Marshal(ts2)
	require.NoError(t, err)
	data3, err := proto.Marshal(ts3)
	require.NoError(t, err)

	require.NoError(t, batch.SetBytes(key1, data1))
	require.NoError(t, batch.SetBytes(key2, data2))
	require.NoError(t, batch.SetBytes(key3, data3))
	require.NoError(t, batch.Commit())

	// Create iterator and proto cursor
	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0xAA},
		UpperBound: []byte{0xAB},
	})
	require.NoError(t, err)

	cursor := NewProtoCursor[*commonpb.Timestamp](iter)
	defer func() { _ = cursor.Close() }()

	// Read items
	got1, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), got1.Data)

	got2, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(2000), got2.Data)

	got3, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(3000), got3.Data)

	// EOF
	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestProtoCursor_Empty(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Create iterator over empty range
	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0xBB},
		UpperBound: []byte{0xBC},
	})
	require.NoError(t, err)

	cursor := NewProtoCursor[*commonpb.Timestamp](iter)
	defer func() { _ = cursor.Close() }()

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}

func TestProtoCursor_CloseNilIter(t *testing.T) {
	t.Parallel()

	// Test Close with nil iterator
	cursor := &ProtoCursor[*commonpb.Timestamp]{}
	require.NoError(t, cursor.Close())
}

func TestProtoCursor_MultipleCallsAfterEOF(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write one item
	batch := s.NewBatch()
	ts := &commonpb.Timestamp{Data: 42}
	data, err := proto.Marshal(ts)
	require.NoError(t, err)

	kb := NewKeyBuilder()
	key := kb.PutByte(0xCC).PutUInt64(1).Build()
	require.NoError(t, batch.SetBytes(key, data))
	require.NoError(t, batch.Commit())

	iter, err := s.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0xCC},
		UpperBound: []byte{0xCD},
	})
	require.NoError(t, err)

	cursor := NewProtoCursor[*commonpb.Timestamp](iter)
	defer func() { _ = cursor.Close() }()

	// Read the one item
	got, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(42), got.Data)

	// Should get EOF
	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)
}
