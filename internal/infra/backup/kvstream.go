package backup

import (
	"encoding/binary"
	"fmt"
	"io"
)

// KV stream binary format:
//   Header:  [4B magic "LBKV"][1B version=1]
//   Entries: [4B key_len][key_bytes][4B value_len][value_bytes] repeated
//   Footer:  [4B 0x00000000] (sentinel)

var kvStreamMagic = [4]byte{'L', 'B', 'K', 'V'}

const kvStreamVersion = 1

// KVStreamWriter writes Pebble KV pairs to an io.Writer in the KV stream format.
type KVStreamWriter struct {
	w   io.Writer
	buf [4]byte
}

// NewKVStreamWriter creates a new KV stream writer.
func NewKVStreamWriter(w io.Writer) *KVStreamWriter {
	return &KVStreamWriter{w: w}
}

// WriteHeader writes the magic bytes and version.
func (w *KVStreamWriter) WriteHeader() error {
	if _, err := w.w.Write(kvStreamMagic[:]); err != nil {
		return fmt.Errorf("writing magic: %w", err)
	}

	w.buf[0] = kvStreamVersion
	if _, err := w.w.Write(w.buf[:1]); err != nil {
		return fmt.Errorf("writing version: %w", err)
	}

	return nil
}

// WriteEntry writes a single key-value pair.
func (w *KVStreamWriter) WriteEntry(key, value []byte) error {
	binary.BigEndian.PutUint32(w.buf[:], uint32(len(key)))
	if _, err := w.w.Write(w.buf[:]); err != nil {
		return fmt.Errorf("writing key length: %w", err)
	}

	if _, err := w.w.Write(key); err != nil {
		return fmt.Errorf("writing key: %w", err)
	}

	binary.BigEndian.PutUint32(w.buf[:], uint32(len(value)))
	if _, err := w.w.Write(w.buf[:]); err != nil {
		return fmt.Errorf("writing value length: %w", err)
	}

	if _, err := w.w.Write(value); err != nil {
		return fmt.Errorf("writing value: %w", err)
	}

	return nil
}

// WriteFooter writes the sentinel (zero-length key) to signal end of stream.
func (w *KVStreamWriter) WriteFooter() error {
	binary.BigEndian.PutUint32(w.buf[:], 0)
	if _, err := w.w.Write(w.buf[:]); err != nil {
		return fmt.Errorf("writing footer: %w", err)
	}

	return nil
}

// KVStreamReader reads Pebble KV pairs from an io.Reader in the KV stream format.
type KVStreamReader struct {
	r   io.Reader
	buf [4]byte
}

// NewKVStreamReader creates a new KV stream reader.
func NewKVStreamReader(r io.Reader) *KVStreamReader {
	return &KVStreamReader{r: r}
}

// ReadHeader reads and validates the magic bytes and version.
func (r *KVStreamReader) ReadHeader() error {
	var magic [4]byte
	if _, err := io.ReadFull(r.r, magic[:]); err != nil {
		return fmt.Errorf("reading magic: %w", err)
	}

	if magic != kvStreamMagic {
		return fmt.Errorf("invalid magic: %x", magic)
	}

	if _, err := io.ReadFull(r.r, r.buf[:1]); err != nil {
		return fmt.Errorf("reading version: %w", err)
	}

	if r.buf[0] != kvStreamVersion {
		return fmt.Errorf("unsupported version: %d", r.buf[0])
	}

	return nil
}

// ReadEntry reads a single key-value pair. Returns io.EOF when the footer sentinel is reached.
func (r *KVStreamReader) ReadEntry() (key, value []byte, err error) {
	if _, err := io.ReadFull(r.r, r.buf[:]); err != nil {
		return nil, nil, fmt.Errorf("reading key length: %w", err)
	}

	keyLen := binary.BigEndian.Uint32(r.buf[:])
	if keyLen == 0 {
		return nil, nil, io.EOF // sentinel
	}

	key = make([]byte, keyLen)
	if _, err := io.ReadFull(r.r, key); err != nil {
		return nil, nil, fmt.Errorf("reading key: %w", err)
	}

	if _, err := io.ReadFull(r.r, r.buf[:]); err != nil {
		return nil, nil, fmt.Errorf("reading value length: %w", err)
	}

	valueLen := binary.BigEndian.Uint32(r.buf[:])

	value = make([]byte, valueLen)
	if _, err := io.ReadFull(r.r, value); err != nil {
		return nil, nil, fmt.Errorf("reading value: %w", err)
	}

	return key, value, nil
}
