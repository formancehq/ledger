package balancehistoryarchive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

const (
	headerSize       = 12
	recordHeaderSize = 12
	trailerSize      = 56
	// EmptyEncodedSize is the exact encoded size of a run containing no
	// records. Callers can use it with EncodedRecordSize to split runs before
	// publication without duplicating codec arithmetic.
	EmptyEncodedSize uint64 = headerSize + trailerSize
	minimumBlobSize         = EmptyEncodedSize
)

var (
	headerMagic  = [8]byte{'F', 'M', 'B', 'H', 'R', 'U', 'N', 0}
	trailerMagic = [8]byte{'F', 'M', 'B', 'H', 'E', 'N', 'D', 0}
)

// Encode writes a deterministic, self-verifying run blob. The returned Ref is
// the content address of exactly the bytes written to w.
func Encode(ctx context.Context, w io.Writer, records RecordStream) (Ref, error) {
	if records == nil {
		return Ref{}, errors.New("record stream is required")
	}

	objectHash := sha256.New()
	prefixHash := sha256.New()
	prefixWriter := io.MultiWriter(w, objectHash, prefixHash)

	header := make([]byte, headerSize)
	copy(header[:8], headerMagic[:])
	binary.BigEndian.PutUint32(header[8:], FormatVersion)
	if err := writeFull(prefixWriter, header); err != nil {
		return Ref{}, fmt.Errorf("writing balance history archive header: %w", err)
	}

	prefixSize := uint64(len(header))
	recordCount := uint64(0)
	var previousKey []byte

	for records.Next() {
		if err := ctx.Err(); err != nil {
			return Ref{}, err
		}

		record := records.Record()
		if recordCount > 0 && bytes.Compare(previousKey, record.Key) >= 0 {
			return Ref{}, fmt.Errorf("%w at record %d", ErrUnsortedRecords, recordCount)
		}
		encodedRecordSize, err := EncodedRecordSize(record)
		if err != nil {
			return Ref{}, fmt.Errorf("record %d: %w", recordCount, err)
		}

		recordHeader := make([]byte, recordHeaderSize)
		binary.BigEndian.PutUint32(recordHeader[:4], uint32(len(record.Key)))
		binary.BigEndian.PutUint64(recordHeader[4:], uint64(len(record.Value)))
		if err := writeFull(prefixWriter, recordHeader); err != nil {
			return Ref{}, fmt.Errorf("writing record %d header: %w", recordCount, err)
		}
		if err := writeFull(prefixWriter, record.Key); err != nil {
			return Ref{}, fmt.Errorf("writing record %d key: %w", recordCount, err)
		}
		if err := writeFull(prefixWriter, record.Value); err != nil {
			return Ref{}, fmt.Errorf("writing record %d value: %w", recordCount, err)
		}

		prefixSize += encodedRecordSize
		recordCount++
		previousKey = bytes.Clone(record.Key)
	}
	if err := records.Err(); err != nil {
		return Ref{}, fmt.Errorf("reading record stream: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return Ref{}, err
	}

	trailer := make([]byte, trailerSize)
	binary.BigEndian.PutUint64(trailer[:8], prefixSize)
	binary.BigEndian.PutUint64(trailer[8:16], recordCount)
	copy(trailer[16:48], prefixHash.Sum(nil))
	copy(trailer[48:], trailerMagic[:])
	if err := writeFull(io.MultiWriter(w, objectHash), trailer); err != nil {
		return Ref{}, fmt.Errorf("writing balance history archive trailer: %w", err)
	}

	ref := Ref{
		Version:     FormatVersion,
		Size:        prefixSize + trailerSize,
		RecordCount: recordCount,
	}
	copy(ref.SHA256[:], objectHash.Sum(nil))

	return ref, nil
}

// EncodedRecordSize returns the exact number of bytes Encode writes for one
// record, including its fixed key/value length header.
func EncodedRecordSize(record Record) (uint64, error) {
	keyLength := uint64(len(record.Key))
	if keyLength > uint64(^uint32(0)) {
		return 0, errors.New("record key exceeds uint32 length")
	}

	valueLength := uint64(len(record.Value))
	size := uint64(recordHeaderSize)
	if keyLength > ^uint64(0)-size {
		return 0, errors.New("encoded record size overflows uint64")
	}
	size += keyLength
	if valueLength > ^uint64(0)-size {
		return 0, errors.New("encoded record size overflows uint64")
	}

	return size + valueLength, nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := w.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}

	return nil
}

// Reader iterates a verified run without retaining its values in memory.
type Reader struct {
	file             *os.File
	ref              Ref
	section          io.Reader
	remainingBytes   uint64
	remainingRecords uint64
	previousKey      []byte
	current          Record
	err              error
	closed           bool
}

// OpenFile verifies the complete object hash, embedded content hash, size,
// format, structure, and sort order before returning a reset streaming reader.
func OpenFile(path string, ref Ref) (*Reader, error) {
	if err := ref.validate(); err != nil {
		return nil, err
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, &CorruptError{Ref: ref, Detail: "opening cached blob", Cause: err}
	}

	reader, _, err := openAndVerify(file, ref, false)
	if err != nil {
		closeErr := file.Close()
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}

		return nil, err
	}

	return reader, nil
}

func openAndVerify(file *os.File, ref Ref, buildIndex bool) (*Reader, *recordIndex, error) {
	stat, err := file.Stat()
	if err != nil {
		return nil, nil, &CorruptError{Ref: ref, Detail: "stating cached blob", Cause: err}
	}
	if stat.Size() < 0 || uint64(stat.Size()) != ref.Size {
		return nil, nil, &CorruptError{
			Ref:    ref,
			Detail: fmt.Sprintf("size mismatch: expected %d, got %d", ref.Size, stat.Size()),
		}
	}

	trailer := make([]byte, trailerSize)
	if _, err := file.ReadAt(trailer, stat.Size()-trailerSize); err != nil {
		return nil, nil, &CorruptError{Ref: ref, Detail: "reading trailer", Cause: err}
	}
	if !bytes.Equal(trailer[48:], trailerMagic[:]) {
		return nil, nil, &CorruptError{Ref: ref, Detail: "trailer magic mismatch"}
	}

	prefixSize := binary.BigEndian.Uint64(trailer[:8])
	recordCount := binary.BigEndian.Uint64(trailer[8:16])
	if prefixSize != ref.Size-trailerSize {
		return nil, nil, &CorruptError{Ref: ref, Detail: "embedded size does not match object size"}
	}
	if recordCount != ref.RecordCount {
		return nil, nil, &CorruptError{
			Ref:    ref,
			Detail: fmt.Sprintf("record count mismatch: expected %d, got %d", ref.RecordCount, recordCount),
		}
	}

	prefixHash := sha256.New()
	fullHash := sha256.New()
	hashedPrefix := io.TeeReader(
		io.NewSectionReader(file, 0, int64(prefixSize)),
		io.MultiWriter(prefixHash, fullHash),
	)
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(hashedPrefix, header); err != nil {
		return nil, nil, &CorruptError{Ref: ref, Detail: "reading header", Cause: err}
	}
	if !bytes.Equal(header[:8], headerMagic[:]) {
		return nil, nil, &CorruptError{Ref: ref, Detail: "header magic mismatch"}
	}
	version := binary.BigEndian.Uint32(header[8:])
	if version != FormatVersion {
		return nil, nil, fmt.Errorf("%w: got version %d, support %d", ErrUnsupportedFormat, version, FormatVersion)
	}
	if prefixSize < headerSize {
		return nil, nil, &CorruptError{Ref: ref, Detail: "encoded content is shorter than its header"}
	}
	recordDataSize := prefixSize - headerSize
	if recordCount > recordDataSize/recordHeaderSize {
		return nil, nil, &CorruptError{Ref: ref, Detail: "record count exceeds encoded content capacity"}
	}

	reader := &Reader{file: file, ref: ref}
	reader.reset(recordDataSize, recordCount)
	reader.section = hashedPrefix
	var index *recordIndex
	if buildIndex {
		index = &recordIndex{
			offsets: make([]uint64, 0, recordCount),
			dataEnd: prefixSize,
		}
	}
	offset := uint64(headerSize)
	for reader.Next() {
		if index != nil {
			index.offsets = append(index.offsets, offset)
		}
		record := reader.Record()
		offset += recordHeaderSize + uint64(len(record.Key)) + uint64(len(record.Value))
	}
	if err := reader.Err(); err != nil {
		return nil, nil, err
	}
	if offset != prefixSize {
		return nil, nil, &CorruptError{Ref: ref, Detail: "decoded record sizes do not match embedded size"}
	}
	if got := prefixHash.Sum(nil); !bytes.Equal(got, trailer[16:48]) {
		return nil, nil, &CorruptError{Ref: ref, Detail: "embedded content sha256 mismatch"}
	}
	if _, err := fullHash.Write(trailer); err != nil {
		return nil, nil, &CorruptError{Ref: ref, Detail: "hashing trailer", Cause: err}
	}
	if got := fullHash.Sum(nil); !bytes.Equal(got, ref.SHA256[:]) {
		return nil, nil, &CorruptError{Ref: ref, Detail: "object sha256 mismatch"}
	}
	reader.reset(recordDataSize, recordCount)

	return reader, index, nil
}

func (r *Reader) reset(dataSize, recordCount uint64) {
	r.section = io.NewSectionReader(r.file, headerSize, int64(dataSize))
	r.remainingBytes = dataSize
	r.remainingRecords = recordCount
	r.previousKey = nil
	r.current = Record{}
	r.err = nil
}

// Next advances to the next record. Key and Value remain valid until the next
// call to Next or Close.
func (r *Reader) Next() bool {
	if r.closed || r.err != nil {
		return false
	}
	if r.remainingRecords == 0 {
		if r.remainingBytes != 0 {
			r.err = &CorruptError{Ref: r.ref, Detail: "trailing bytes after final record"}
		}

		return false
	}
	if r.remainingBytes < recordHeaderSize {
		r.err = &CorruptError{Ref: r.ref, Detail: "truncated record header"}

		return false
	}

	header := make([]byte, recordHeaderSize)
	if _, err := io.ReadFull(r.section, header); err != nil {
		r.err = &CorruptError{Ref: r.ref, Detail: "reading record header", Cause: err}

		return false
	}
	r.remainingBytes -= recordHeaderSize

	keyLength := uint64(binary.BigEndian.Uint32(header[:4]))
	valueLength := binary.BigEndian.Uint64(header[4:])
	if keyLength > r.remainingBytes || valueLength > r.remainingBytes-keyLength {
		r.err = &CorruptError{Ref: r.ref, Detail: "record length exceeds encoded content"}

		return false
	}
	maxInt := uint64(^uint(0) >> 1)
	if keyLength > maxInt || valueLength > maxInt {
		r.err = &CorruptError{Ref: r.ref, Detail: "record length exceeds platform allocation limit"}

		return false
	}

	key := make([]byte, int(keyLength))
	if _, err := io.ReadFull(r.section, key); err != nil {
		r.err = &CorruptError{Ref: r.ref, Detail: "reading record key", Cause: err}

		return false
	}
	value := make([]byte, int(valueLength))
	if _, err := io.ReadFull(r.section, value); err != nil {
		r.err = &CorruptError{Ref: r.ref, Detail: "reading record value", Cause: err}

		return false
	}

	if r.previousKey != nil && bytes.Compare(r.previousKey, key) >= 0 {
		r.err = &CorruptError{Ref: r.ref, Detail: "records are not strictly sorted"}

		return false
	}

	r.remainingBytes -= keyLength + valueLength
	r.remainingRecords--
	r.previousKey = bytes.Clone(key)
	r.current = Record{Key: key, Value: value}

	return true
}

// Record returns the current decoded record.
func (r *Reader) Record() Record {
	return r.current
}

// Err reports the first structural or I/O error encountered while iterating.
func (r *Reader) Err() error {
	if r.err == nil {
		return nil
	}
	if errors.Is(r.err, ErrCorrupt) {
		return r.err
	}

	return &CorruptError{Ref: r.ref, Detail: "decoding records", Cause: r.err}
}

// Close releases the underlying file descriptor.
func (r *Reader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	return r.file.Close()
}

func verifyFile(path string, ref Ref) error {
	reader, err := OpenFile(path, ref)
	if err != nil {
		return err
	}

	return reader.Close()
}
