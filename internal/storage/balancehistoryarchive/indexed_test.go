package balancehistoryarchive

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
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

func TestIndexedReaderRejectsInvalidFilesAndIndexes(t *testing.T) {
	t.Parallel()

	_, ref := encodeBlob(t, testRecords("missing-index", 8))
	missing := filepath.Join(t.TempDir(), "missing")
	_, err := buildRecordIndex(missing, ref)
	require.ErrorContains(t, err, "opening cached blob for indexing")
	_, err = openIndexedReader(missing, ref, &recordIndex{})
	require.ErrorContains(t, err, "opening indexed cached blob")
	_, err = openIndexedReader(missing, ref, nil)
	require.ErrorContains(t, err, "record index is required")

	blob, ref := encodeBlob(t, testRecords("indexed-size", 8))
	path := filepath.Join(t.TempDir(), "run")
	require.NoError(t, os.WriteFile(path, blob, 0o600))
	wrongSize := ref
	wrongSize.Size++
	_, err = openIndexedReader(path, wrongSize, &recordIndex{})
	require.ErrorContains(t, err, "indexed size mismatch")
}

func TestIndexedReaderFailsClosedForCorruptOffsetsAndRecords(t *testing.T) {
	t.Parallel()

	blob, ref := encodeBlob(t, testRecords("corrupt-index", 8))
	path := filepath.Join(t.TempDir(), "run")
	require.NoError(t, os.WriteFile(path, blob, 0o600))

	t.Run("offset outside data", func(t *testing.T) {
		reader, err := openIndexedReader(path, ref, &recordIndex{
			offsets: []uint64{ref.Size},
			dataEnd: ref.Size - trailerSize,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, reader.Close()) }()

		require.False(t, reader.SeekGE(nil))
		require.ErrorContains(t, reader.Err(), "record header is outside encoded content")
		require.False(t, reader.SeekLT([]byte("z")))
	})

	t.Run("record length outside data", func(t *testing.T) {
		mutated := bytes.Clone(blob)
		binary.BigEndian.PutUint32(mutated[headerSize:headerSize+4], ^uint32(0))
		mutatedPath := filepath.Join(t.TempDir(), "run")
		require.NoError(t, os.WriteFile(mutatedPath, mutated, 0o600))
		reader, err := openIndexedReader(mutatedPath, ref, &recordIndex{
			offsets: []uint64{headerSize},
			dataEnd: ref.Size - trailerSize,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, reader.Close()) }()

		require.False(t, reader.SeekGE(nil))
		require.ErrorContains(t, reader.Err(), "record length exceeds encoded content")
	})

	t.Run("truncated payload", func(t *testing.T) {
		const size = minimumBlobSize + 8
		contents := make([]byte, size)
		offset := size - 16
		binary.BigEndian.PutUint32(contents[offset:offset+4], 4)
		binary.BigEndian.PutUint64(contents[offset+4:offset+12], 4)
		truncatedPath := filepath.Join(t.TempDir(), "run")
		require.NoError(t, os.WriteFile(truncatedPath, contents, 0o600))
		truncatedRef := Ref{Version: FormatVersion, Size: size, SHA256: [32]byte{1}}
		reader, err := openIndexedReader(truncatedPath, truncatedRef, &recordIndex{
			offsets: []uint64{offset},
			dataEnd: size + 8,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, reader.Close()) }()

		require.False(t, reader.SeekGE(nil))
		require.ErrorContains(t, reader.Err(), "reading indexed record")
	})
}

func TestIndexedReaderStateGuards(t *testing.T) {
	t.Parallel()

	closed := &IndexedReader{closed: true}
	require.False(t, closed.SeekGE(nil))
	require.False(t, closed.SeekLT(nil))
	require.False(t, closed.Next())

	failed := &IndexedReader{err: os.ErrInvalid}
	require.False(t, failed.SeekGE(nil))
	require.False(t, failed.SeekLT(nil))
	require.False(t, failed.Next())

	reader := &IndexedReader{index: &recordIndex{}, ordinal: -1}
	_, err := reader.keyAt(0)
	require.ErrorContains(t, err, "ordinal is out of bounds")
	require.False(t, reader.Next())
}
