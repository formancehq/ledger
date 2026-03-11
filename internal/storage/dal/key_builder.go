package dal

import "encoding/binary"

// KeyBuilder constructs Pebble and Pebble keys by appending typed components
// into a reusable byte slice. All write methods return *KeyBuilder for chaining.
type KeyBuilder struct {
	buf []byte
}

// NewKeyBuilder creates a new KeyBuilder with preallocated capacity.
func NewKeyBuilder() *KeyBuilder {
	return &KeyBuilder{buf: make([]byte, 0, 256)}
}

// Reset clears the builder for reuse.
func (kb *KeyBuilder) Reset() *KeyBuilder {
	kb.buf = kb.buf[:0]

	return kb
}

// PutByte appends a single byte.
func (kb *KeyBuilder) PutByte(value byte) *KeyBuilder {
	kb.buf = append(kb.buf, value)

	return kb
}

// PutBytes appends raw bytes.
func (kb *KeyBuilder) PutBytes(value []byte) *KeyBuilder {
	kb.buf = append(kb.buf, value...)

	return kb
}

// PutString appends a raw string.
func (kb *KeyBuilder) PutString(value string) *KeyBuilder {
	kb.buf = append(kb.buf, value...)

	return kb
}

// PutStringNull appends a string followed by a null terminator.
func (kb *KeyBuilder) PutStringNull(s string) *KeyBuilder {
	kb.buf = append(kb.buf, s...)
	kb.buf = append(kb.buf, 0x00)

	return kb
}

// PutUint64 appends a uint64 in big-endian order.
func (kb *KeyBuilder) PutUint64(v uint64) *KeyBuilder {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	kb.buf = append(kb.buf, buf[:]...)

	return kb
}

// PutUint32 appends a uint32 in big-endian order.
func (kb *KeyBuilder) PutUint32(v uint32) *KeyBuilder {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	kb.buf = append(kb.buf, buf[:]...)

	return kb
}

// PutLedgerName appends a ledger name followed by a null terminator.
func (kb *KeyBuilder) PutLedgerName(name string) *KeyBuilder {
	return kb.PutStringNull(name)
}

// PutNamespace appends a namespace prefix (e.g., "a:" or "t:").
func (kb *KeyBuilder) PutNamespace(ns string) *KeyBuilder {
	return kb.PutString(ns)
}

// Build returns a copy of the built key and resets the buffer for reuse.
func (kb *KeyBuilder) Build() []byte {
	result := make([]byte, len(kb.buf))
	copy(result, kb.buf)
	kb.buf = kb.buf[:0]

	return result
}

// Snapshot returns a copy of the current key state without resetting.
func (kb *KeyBuilder) Snapshot() []byte {
	result := make([]byte, len(kb.buf))
	copy(result, kb.buf)

	return result
}

// Len returns the current length of the key being built.
func (kb *KeyBuilder) Len() int {
	return len(kb.buf)
}
