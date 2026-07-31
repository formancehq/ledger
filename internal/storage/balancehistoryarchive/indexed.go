package balancehistoryarchive

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

type recordIndex struct {
	offsets []uint64
	dataEnd uint64
}

func buildRecordIndex(path string, ref Ref) (*recordIndex, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, &CorruptError{Ref: ref, Detail: "opening cached blob for indexing", Cause: err}
	}

	reader, index, err := openAndVerify(file, ref, true)
	if err != nil {
		closeErr := file.Close()
		if closeErr != nil {
			err = errors.Join(err, closeErr)
		}

		return nil, err
	}
	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("closing indexed balance history archive: %w", err)
	}

	return index, nil
}

// IndexedReader performs logarithmic seeks over an immutable run while values
// remain disk-backed. It is not safe for concurrent method calls; callers can
// open independent readers under the same Lease when concurrent cursors are
// required.
type IndexedReader struct {
	file    *os.File
	ref     Ref
	index   *recordIndex
	current Record
	ordinal int
	valid   bool
	err     error
	closed  bool
}

func openIndexedReader(path string, ref Ref, index *recordIndex) (*IndexedReader, error) {
	if index == nil {
		return nil, errors.New("invariant: balance history archive record index is required")
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, &CorruptError{Ref: ref, Detail: "opening indexed cached blob", Cause: err}
	}
	stat, err := file.Stat()
	if err != nil {
		closeErr := file.Close()

		return nil, errors.Join(&CorruptError{Ref: ref, Detail: "stating indexed cached blob", Cause: err}, closeErr)
	}
	if stat.Size() < 0 || uint64(stat.Size()) != ref.Size {
		closeErr := file.Close()
		err := &CorruptError{
			Ref:    ref,
			Detail: fmt.Sprintf("indexed size mismatch: expected %d, got %d", ref.Size, stat.Size()),
		}

		return nil, errors.Join(err, closeErr)
	}

	return &IndexedReader{file: file, ref: ref, index: index, ordinal: -1}, nil
}

// SeekGE positions on the first record whose key is greater than or equal to
// key. It returns false when no such record exists or an error occurs.
func (r *IndexedReader) SeekGE(key []byte) bool {
	if !r.usable() {
		return false
	}

	low, high := 0, len(r.index.offsets)
	for low < high {
		middle := low + (high-low)/2
		candidate, err := r.keyAt(middle)
		if err != nil {
			r.err = err
			r.clear()

			return false
		}
		if bytes.Compare(candidate, key) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low == len(r.index.offsets) {
		r.clear()

		return false
	}

	return r.load(low)
}

// SeekLT positions on the last record whose key is strictly less than key. It
// returns false when no such record exists or an error occurs.
func (r *IndexedReader) SeekLT(key []byte) bool {
	if !r.usable() {
		return false
	}

	low, high := 0, len(r.index.offsets)
	for low < high {
		middle := low + (high-low)/2
		candidate, err := r.keyAt(middle)
		if err != nil {
			r.err = err
			r.clear()

			return false
		}
		if bytes.Compare(candidate, key) < 0 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low == 0 {
		r.clear()

		return false
	}

	return r.load(low - 1)
}

// Next advances from the current seek position to the following record.
func (r *IndexedReader) Next() bool {
	if !r.usable() || !r.valid {
		return false
	}
	if r.ordinal+1 >= len(r.index.offsets) {
		r.clear()

		return false
	}

	return r.load(r.ordinal + 1)
}

// Record returns the record at the current seek position.
func (r *IndexedReader) Record() Record {
	return r.current
}

// Err reports the first random-access or decoding error.
func (r *IndexedReader) Err() error {
	return r.err
}

// Close releases the indexed reader's file descriptor. It does not release
// the parent Lease.
func (r *IndexedReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.clear()

	return r.file.Close()
}

func (r *IndexedReader) usable() bool {
	return !r.closed && r.err == nil
}

func (r *IndexedReader) clear() {
	r.current = Record{}
	r.ordinal = -1
	r.valid = false
}

func (r *IndexedReader) keyAt(ordinal int) ([]byte, error) {
	offset, keyLength, _, err := r.recordHeader(ordinal)
	if err != nil {
		return nil, err
	}

	key := make([]byte, int(keyLength))
	if _, err := r.file.ReadAt(key, int64(offset+recordHeaderSize)); err != nil {
		return nil, &CorruptError{Ref: r.ref, Detail: "reading indexed record key", Cause: err}
	}

	return key, nil
}

func (r *IndexedReader) load(ordinal int) bool {
	offset, keyLength, valueLength, err := r.recordHeader(ordinal)
	if err != nil {
		r.err = err
		r.clear()

		return false
	}

	payloadLength := keyLength + valueLength
	payload := make([]byte, int(payloadLength))
	if _, err := r.file.ReadAt(payload, int64(offset+recordHeaderSize)); err != nil {
		r.err = &CorruptError{Ref: r.ref, Detail: "reading indexed record", Cause: err}
		r.clear()

		return false
	}

	r.current = Record{Key: payload[:keyLength], Value: payload[keyLength:]}
	r.ordinal = ordinal
	r.valid = true

	return true
}

func (r *IndexedReader) recordHeader(ordinal int) (uint64, uint64, uint64, error) {
	if ordinal < 0 || ordinal >= len(r.index.offsets) {
		return 0, 0, 0, errors.New("invariant: balance history archive record ordinal is out of bounds")
	}

	offset := r.index.offsets[ordinal]
	if offset > r.index.dataEnd || r.index.dataEnd-offset < recordHeaderSize {
		return 0, 0, 0, &CorruptError{Ref: r.ref, Detail: "indexed record header is outside encoded content"}
	}
	header := make([]byte, recordHeaderSize)
	if _, err := r.file.ReadAt(header, int64(offset)); err != nil {
		return 0, 0, 0, &CorruptError{Ref: r.ref, Detail: "reading indexed record header", Cause: err}
	}

	keyLength := uint64(binary.BigEndian.Uint32(header[:4]))
	valueLength := binary.BigEndian.Uint64(header[4:])
	remaining := r.index.dataEnd - offset - recordHeaderSize
	if keyLength > remaining || valueLength > remaining-keyLength {
		return 0, 0, 0, &CorruptError{Ref: r.ref, Detail: "indexed record length exceeds encoded content"}
	}
	maxInt := uint64(^uint(0) >> 1)
	if keyLength > maxInt || valueLength > maxInt-keyLength {
		return 0, 0, 0, &CorruptError{Ref: r.ref, Detail: "indexed record length exceeds platform allocation limit"}
	}

	return offset, keyLength, valueLength, nil
}

var _ io.Closer = (*IndexedReader)(nil)
