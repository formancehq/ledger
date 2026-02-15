package data

import (
	"bytes"
	"encoding/binary"
)

type CanonicalBytes interface {
	Bytes() []byte
	Unmarshal(data []byte) error
}

type KeyBuilder struct {
	buf *bytes.Buffer
	err error
}

func (kb *KeyBuilder) Reset() {
	kb.buf.Reset()
}

func (kb *KeyBuilder) PutUInt64(v uint64) *KeyBuilder {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	if _, err := kb.buf.Write(buf[:]); err != nil {
		kb.err = err
	}
	return kb
}

func (kb *KeyBuilder) PutUInt32(v uint32) *KeyBuilder {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	if _, err := kb.buf.Write(buf[:]); err != nil {
		kb.err = err
	}
	return kb
}

func (kb *KeyBuilder) PutString(value string) *KeyBuilder {
	if _, err := kb.buf.WriteString(value); err != nil {
		kb.err = err
	}
	return kb
}

func (kb *KeyBuilder) PutByte(value byte) *KeyBuilder {
	if err := kb.buf.WriteByte(value); err != nil {
		kb.err = err
	}
	return kb
}

func (kb *KeyBuilder) PutBytes(value []byte) *KeyBuilder {
	if _, err := kb.buf.Write(value); err != nil {
		kb.err = err
	}
	return kb
}

func (kb *KeyBuilder) PutLedgerPrefix(ledgerID uint32) *KeyBuilder {
	return kb.PutUInt32(ledgerID)
}

// Build returns a copy of the built key and resets the buffer for reuse.
func (kb *KeyBuilder) Build() []byte {
	if kb.err != nil {
		panic(kb.err)
	}
	defer kb.Reset()
	return bytes.Clone(kb.buf.Bytes())
}

// Snapshot returns a copy of the current key without resetting the buffer.
// Useful when you need to continue building from the current state.
func (kb *KeyBuilder) Snapshot() []byte {
	return bytes.Clone(kb.buf.Bytes())
}

func NewKeyBuilder() *KeyBuilder {
	return &KeyBuilder{
		buf: bytes.NewBuffer(make([]byte, 0, 1024)),
	}
}
