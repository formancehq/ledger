package balancehistoryarchive

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
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

type failingArchiveWriter struct {
	remaining int
	err       error
}

func (w *failingArchiveWriter) Write(data []byte) (int, error) {
	if w.remaining == 0 {
		return 0, w.err
	}
	if len(data) <= w.remaining {
		w.remaining -= len(data)

		return len(data), nil
	}
	written := w.remaining
	w.remaining = 0

	return written, nil
}

type failingRecordStream struct {
	err error
}

func (s *failingRecordStream) Next() bool     { return false }
func (s *failingRecordStream) Record() Record { return Record{} }
func (s *failingRecordStream) Err() error     { return s.err }

func TestEncodeReportsStreamAndWriterFailures(t *testing.T) {
	t.Parallel()

	_, err := Encode(context.Background(), &bytes.Buffer{}, nil)
	require.ErrorContains(t, err, "record stream is required")

	wantErr := errors.New("stream failed")
	_, err = Encode(context.Background(), &bytes.Buffer{}, &failingRecordStream{err: wantErr})
	require.ErrorIs(t, err, wantErr)
	require.ErrorContains(t, err, "reading record stream")

	record := Record{Key: []byte("key"), Value: []byte("value")}
	tests := []struct {
		name      string
		allowance int
		want      string
	}{
		{name: "header", allowance: 0, want: "writing balance history archive header"},
		{name: "record header", allowance: headerSize, want: "writing record 0 header"},
		{name: "record key", allowance: headerSize + recordHeaderSize, want: "writing record 0 key"},
		{
			name:      "record value",
			allowance: headerSize + recordHeaderSize + len(record.Key),
			want:      "writing record 0 value",
		},
		{
			name:      "trailer",
			allowance: headerSize + recordHeaderSize + len(record.Key) + len(record.Value),
			want:      "writing balance history archive trailer",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			writeErr := errors.New("write failed")
			_, encodeErr := Encode(
				context.Background(),
				&failingArchiveWriter{remaining: test.allowance, err: writeErr},
				NewSliceStream([]Record{record}),
			)
			require.ErrorIs(t, encodeErr, writeErr)
			require.ErrorContains(t, encodeErr, test.want)
		})
	}

	err = writeFull(zeroArchiveWriter{}, []byte("data"))
	require.ErrorIs(t, err, io.ErrShortWrite)
}

type zeroArchiveWriter struct{}

func (zeroArchiveWriter) Write([]byte) (int, error) { return 0, nil }

func TestOpenFileRejectsMalformedEnvelope(t *testing.T) {
	t.Parallel()

	validBlob, validRef := encodeBlob(t, []Record{{Key: []byte("key"), Value: []byte("value")}})
	tests := []struct {
		name   string
		mutate func(blob []byte, ref *Ref)
		want   string
	}{
		{
			name: "size mismatch",
			mutate: func(_ []byte, ref *Ref) {
				ref.Size++
			},
			want: "size mismatch",
		},
		{
			name: "trailer magic",
			mutate: func(blob []byte, ref *Ref) {
				blob[len(blob)-1] ^= 0xff
				ref.SHA256 = sha256Bytes(blob)
			},
			want: "trailer magic mismatch",
		},
		{
			name: "embedded size",
			mutate: func(blob []byte, ref *Ref) {
				binary.BigEndian.PutUint64(blob[len(blob)-trailerSize:], 1)
				ref.SHA256 = sha256Bytes(blob)
			},
			want: "embedded size does not match object size",
		},
		{
			name: "record count",
			mutate: func(blob []byte, ref *Ref) {
				binary.BigEndian.PutUint64(blob[len(blob)-trailerSize+8:], ref.RecordCount+1)
				ref.SHA256 = sha256Bytes(blob)
			},
			want: "record count mismatch",
		},
		{
			name: "header magic",
			mutate: func(blob []byte, ref *Ref) {
				blob[0] ^= 0xff
				ref.SHA256 = sha256Bytes(blob)
			},
			want: "header magic mismatch",
		},
		{
			name: "unsupported version",
			mutate: func(blob []byte, ref *Ref) {
				binary.BigEndian.PutUint32(blob[8:12], FormatVersion+1)
				ref.SHA256 = sha256Bytes(blob)
			},
			want: "unsupported balance history archive format",
		},
		{
			name: "record capacity",
			mutate: func(blob []byte, ref *Ref) {
				count := uint64(100)
				binary.BigEndian.PutUint64(blob[len(blob)-trailerSize+8:], count)
				ref.RecordCount = count
				ref.SHA256 = sha256Bytes(blob)
			},
			want: "record count exceeds encoded content capacity",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			blob := bytes.Clone(validBlob)
			ref := validRef
			test.mutate(blob, &ref)
			path := filepath.Join(t.TempDir(), "run")
			require.NoError(t, os.WriteFile(path, blob, 0o600))
			_, err := OpenFile(path, ref)
			require.ErrorContains(t, err, test.want)
		})
	}

	_, err := OpenFile(filepath.Join(t.TempDir(), "missing"), validRef)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestReaderRejectsMalformedRecordData(t *testing.T) {
	t.Parallel()

	recordHeader := func(keyLength uint32, valueLength uint64) []byte {
		header := make([]byte, recordHeaderSize)
		binary.BigEndian.PutUint32(header[:4], keyLength)
		binary.BigEndian.PutUint64(header[4:], valueLength)

		return header
	}
	tests := []struct {
		name      string
		data      []byte
		bytes     uint64
		records   uint64
		previous  []byte
		presetErr error
		want      string
	}{
		{name: "trailing bytes", data: []byte{1}, bytes: 1, want: "trailing bytes"},
		{name: "truncated header", data: []byte{1}, bytes: 1, records: 1, want: "truncated record header"},
		{
			name:    "record length exceeds content",
			data:    recordHeader(2, 0),
			bytes:   recordHeaderSize + 1,
			records: 1,
			want:    "record length exceeds encoded content",
		},
		{
			name:    "record key read",
			data:    recordHeader(1, 0),
			bytes:   recordHeaderSize + 1,
			records: 1,
			want:    "reading record key",
		},
		{
			name:     "unsorted",
			data:     append(recordHeader(1, 0), 'a'),
			bytes:    recordHeaderSize + 1,
			records:  1,
			previous: []byte("b"),
			want:     "records are not strictly sorted",
		},
		{
			name:      "wrapped decoder error",
			presetErr: errors.New("decoder failed"),
			want:      "decoding records",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			reader := &Reader{
				ref:              Ref{Version: FormatVersion, Size: minimumBlobSize},
				section:          bytes.NewReader(test.data),
				remainingBytes:   test.bytes,
				remainingRecords: test.records,
				previousKey:      test.previous,
				err:              test.presetErr,
			}
			require.False(t, reader.Next())
			require.ErrorContains(t, reader.Err(), test.want)
		})
	}

	reader := &Reader{closed: true}
	require.False(t, reader.Next())
	require.NoError(t, reader.Err())
}
