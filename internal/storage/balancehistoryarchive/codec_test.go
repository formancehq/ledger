package balancehistoryarchive

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCodecIsDeterministicAndStreaming(t *testing.T) {
	t.Parallel()

	records := []Record{
		{Key: []byte("account/001"), Value: []byte("first")},
		{Key: []byte("account/002"), Value: bytes.Repeat([]byte{0x42}, 1024)},
		{Key: []byte("asset/001"), Value: []byte{}},
	}

	var first bytes.Buffer
	firstRef, err := Encode(context.Background(), &first, NewSliceStream(records))
	require.NoError(t, err)

	var second bytes.Buffer
	secondRef, err := Encode(context.Background(), &second, NewSliceStream(records))
	require.NoError(t, err)
	require.Equal(t, firstRef, secondRef)
	require.Equal(t, first.Bytes(), second.Bytes())
	require.Equal(t, uint64(len(first.Bytes())), firstRef.Size)
	require.Equal(t, uint64(len(records)), firstRef.RecordCount)
	expectedSize := EmptyEncodedSize
	for _, record := range records {
		recordSize, err := EncodedRecordSize(record)
		require.NoError(t, err)
		expectedSize += recordSize
	}
	require.Equal(t, expectedSize, firstRef.Size)

	path := filepath.Join(t.TempDir(), "run")
	require.NoError(t, os.WriteFile(path, first.Bytes(), 0o600))
	reader, err := OpenFile(path, firstRef)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reader.Close()) })

	decoded := make([]Record, 0, len(records))
	for reader.Next() {
		record := reader.Record()
		decoded = append(decoded, Record{Key: bytes.Clone(record.Key), Value: bytes.Clone(record.Value)})
	}
	require.NoError(t, reader.Err())
	require.Equal(t, records, decoded)
}

func TestEncodedRecordSizeIncludesFixedHeader(t *testing.T) {
	t.Parallel()

	size, err := EncodedRecordSize(Record{Key: []byte("key"), Value: []byte("value")})
	require.NoError(t, err)
	require.Equal(t, uint64(recordHeaderSize+len("key")+len("value")), size)
	require.Equal(t, uint64(headerSize+trailerSize), EmptyEncodedSize)
}

func TestCodecRejectsUnsortedRecords(t *testing.T) {
	t.Parallel()

	records := []Record{
		{Key: []byte("b"), Value: []byte("first")},
		{Key: []byte("a"), Value: []byte("second")},
	}

	_, err := Encode(context.Background(), &bytes.Buffer{}, NewSliceStream(records))
	require.ErrorIs(t, err, ErrUnsortedRecords)
}

func TestCodecRejectsTamperedEmbeddedHashEvenWithMatchingObjectAddress(t *testing.T) {
	t.Parallel()

	blob, ref := encodeBlob(t, testRecords("tamper", 64))
	blob[len(blob)-trailerSize+16] ^= 0xff
	ref.SHA256 = sha256Bytes(blob)

	path := filepath.Join(t.TempDir(), "run")
	require.NoError(t, os.WriteFile(path, blob, 0o600))
	_, err := OpenFile(path, ref)
	require.ErrorIs(t, err, ErrCorrupt)
}

func TestCodecHonorsCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := Encode(ctx, &bytes.Buffer{}, NewSliceStream(testRecords("cancel", 8)))
	require.True(t, errors.Is(err, context.Canceled))
}
