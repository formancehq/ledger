package balancehistoryarchive

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexedReaderSeekGESeekLTAndNext(t *testing.T) {
	t.Parallel()

	records := []Record{
		{Key: []byte("account/001/100"), Value: []byte("a-100")},
		{Key: []byte("account/001/200"), Value: []byte("a-200")},
		{Key: []byte("account/001/300"), Value: []byte("a-300")},
		{Key: []byte("account/002/100"), Value: []byte("b-100")},
	}
	cold := newMemoryColdStorage(t)
	blob, ref := encodeBlob(t, records)
	cold.seed("cluster/balance-history/nodes/node-1/runs/"+ref.Hex(), archiveChapterID, blob, ref.SHA256[:])
	store := newTestStore(t, cold, t.TempDir(), 1<<20)
	lease, err := store.Fetch(context.Background(), ref)
	require.NoError(t, err)
	defer func() { require.NoError(t, lease.Close()) }()

	reader, err := lease.OpenIndexed()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()

	require.True(t, reader.SeekGE([]byte("account/001/150")))
	require.Equal(t, records[1], cloneRecord(reader.Record()))
	require.True(t, reader.Next())
	require.Equal(t, records[2], cloneRecord(reader.Record()))

	require.True(t, reader.SeekLT([]byte("account/001/300")))
	require.Equal(t, records[1], cloneRecord(reader.Record()))
	require.True(t, reader.SeekLT([]byte("account/001/301")))
	require.Equal(t, records[2], cloneRecord(reader.Record()))

	require.True(t, reader.SeekGE(records[0].Key))
	require.Equal(t, records[0], cloneRecord(reader.Record()))
	require.False(t, reader.SeekLT(records[0].Key))
	require.NoError(t, reader.Err())
	require.False(t, reader.SeekGE([]byte("zzzz")))
	require.NoError(t, reader.Err())
}

func TestIndexedReadersShareCompactIndexAndKeepIndependentPositions(t *testing.T) {
	t.Parallel()

	records := testRecords("shared-index", 128)
	cold := newMemoryColdStorage(t)
	blob, ref := encodeBlob(t, records)
	cold.seed("cluster/balance-history/nodes/node-1/runs/"+ref.Hex(), archiveChapterID, blob, ref.SHA256[:])
	store := newTestStore(t, cold, t.TempDir(), 1<<20)
	lease, err := store.Fetch(context.Background(), ref)
	require.NoError(t, err)
	defer func() { require.NoError(t, lease.Close()) }()

	first, err := lease.OpenIndexed()
	require.NoError(t, err)
	defer func() { require.NoError(t, first.Close()) }()
	second, err := lease.OpenIndexed()
	require.NoError(t, err)
	defer func() { require.NoError(t, second.Close()) }()

	require.True(t, first.SeekGE(records[0].Key))
	require.True(t, second.SeekLT(append(bytes.Clone(records[1].Key), 0)))
	require.Equal(t, records[0], cloneRecord(first.Record()))
	require.Equal(t, records[1], cloneRecord(second.Record()))
	require.Equal(t, int64(ref.Size)+int64(ref.RecordCount)*8, store.CacheStats().Bytes)
	require.Equal(t, 1, cold.fetchCount())
}

func cloneRecord(record Record) Record {
	return Record{Key: bytes.Clone(record.Key), Value: bytes.Clone(record.Value)}
}
