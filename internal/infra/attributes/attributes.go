package attributes

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Attribute is the implementation for all attribute types.
// It holds the Pebble key layout, serialization helpers, and entry write logic.
//
// Key layout: [KeyPrefixAttributes (1B)][AttrType (1B)][CanonicalKey (NB)]
// The AttrType byte is at a fixed offset (position 1), enabling efficient
// type-scoped Pebble range scans without scanning unrelated attribute types.
// Each canonical key has at most one Pebble entry; Set overwrites in place.
//
// Thread-safety:
// - Each Attribute instance has its own pre-allocated key buffer.
// - Use dependency injection (New) to get separate instances per Raft node.
// - Read methods (Get, ScanEntries) allocate their own buffer for concurrent access.
type Attribute[V proto.Message] struct {
	prefix      byte
	newValue    func() V
	keyBuf      []byte // pre-allocated buffer for write-path key construction (reused across calls)
	protoBuffer []byte
}

// ensureKeyBuf ensures keyBuf can hold at least n bytes.
func (a *Attribute[V]) ensureKeyBuf(n int) {
	if len(a.keyBuf) < n {
		a.keyBuf = make([]byte, n)
	}
}

// putPrefix writes [KeyPrefixAttributes][a.prefix][canonicalKey] into buf.
// buf must have at least 2+len(canonicalKey) bytes.
func (a *Attribute[V]) putPrefix(buf []byte, canonicalKey []byte) {
	buf[0] = dal.KeyPrefixAttributes
	buf[1] = a.prefix
	copy(buf[2:], canonicalKey)
}

// prefixLen returns the number of bytes for [KeyPrefixAttributes][attrType][canonicalKey].
func prefixLen(canonicalKey []byte) int {
	return 2 + len(canonicalKey) // 1 for KeyPrefixAttributes + 1 for attrType + N for canonicalKey
}

// vtSizedMarshaler is implemented by vtprotobuf-generated messages.
type vtSizedMarshaler interface {
	SizeVT() int
	MarshalToVT([]byte) (int, error)
}

// vtUnmarshaler is implemented by vtprotobuf-generated messages.
type vtUnmarshaler interface {
	UnmarshalVT([]byte) error
}

// marshalProto marshals a proto message using vtprotobuf when available,
// falling back to standard proto.MarshalOptions otherwise.
// The provided buf is reused when large enough; the returned slice may be a
// different backing array.
func marshalProto(buf []byte, msg proto.Message) ([]byte, error) {
	if m, ok := msg.(vtSizedMarshaler); ok {
		size := m.SizeVT()
		if cap(buf) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}

		n, err := m.MarshalToVT(buf)

		return buf[:n], err
	}

	return proto.MarshalOptions{}.MarshalAppend(buf[:0], msg)
}

// unmarshalProto unmarshals data into a proto message using vtprotobuf when
// available, falling back to standard proto.Unmarshal otherwise.
func unmarshalProto(data []byte, msg proto.Message) error {
	if m, ok := msg.(vtUnmarshaler); ok {
		return m.UnmarshalVT(data)
	}

	return proto.Unmarshal(data, msg)
}

// Set stores a value for the given canonical key and returns the marshaled bytes.
// Key format: [KeyPrefixAttributes][prefix][canonicalKey].
// A simple Set overwrites the previous value in place — no delete needed.
// Uses the pre-allocated keyBuf — not safe for concurrent use.
// The returned slice is valid until the next Set call on this Attribute.
func (a *Attribute[V]) Set(batch *dal.Batch, canonicalKey []byte, value V) ([]byte, error) {
	pLen := prefixLen(canonicalKey)
	a.ensureKeyBuf(pLen)
	a.putPrefix(a.keyBuf, canonicalKey)

	valueBytes, err := marshalProto(a.protoBuffer, value)
	if err != nil {
		return nil, fmt.Errorf("marshaling value: %w", err)
	}

	a.protoBuffer = valueBytes

	return valueBytes, batch.Set(a.keyBuf[:pLen], valueBytes, pebble.NoSync)
}

// AttrTypeLen is the size of the attribute type byte in a Pebble key: 1 byte.
// Kept as a named constant for readability in key length checks.
const AttrTypeLen = 1

// SuffixLen is an alias for AttrTypeLen, preserved for call sites that
// use it in minimum-key-length checks (the numeric value is unchanged).
// Deprecated: prefer AttrTypeLen for new code.
const SuffixLen = AttrTypeLen

// Get retrieves the value for the given canonical key.
// Key format: [KeyPrefixAttributes][attrType][canonicalKey].
// Returns the value and any error. Returns (zero, nil) if no entry found.
// Note: This is a read operation — allocates its own buffer for concurrent safety.
func (a *Attribute[V]) Get(reader dal.PebbleReader, canonicalKey []byte) (V, error) {
	var zeroValue V

	pLen := prefixLen(canonicalKey)
	buf := make([]byte, pLen)
	a.putPrefix(buf, canonicalKey)

	valueBytes, closer, err := reader.Get(buf)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return zeroValue, nil
		}

		return zeroValue, fmt.Errorf("reading key: %w", err)
	}

	defer func() { _ = closer.Close() }()

	v := a.newValue()
	if err := unmarshalProto(valueBytes, v); err != nil {
		return zeroValue, fmt.Errorf("unmarshaling value: %w", err)
	}

	return v, nil
}

// Delete removes the entry for the given canonical key.
// This performs a point delete on the single Pebble key.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own instance.
func (a *Attribute[V]) Delete(batch *dal.Batch, canonicalKey []byte) error {
	pLen := prefixLen(canonicalKey)
	a.ensureKeyBuf(pLen)
	a.putPrefix(a.keyBuf, canonicalKey)

	return batch.DeleteKey(a.keyBuf[:pLen])
}

// ScanResult holds the results of scanning the entry for a canonical key.
type ScanResult[V proto.Message] struct {
	LatestBase V
	HasBase    bool
}

// ScanEntries reads the entry for a canonical key and returns the result.
// Thread-safe: allocates its own buffer for concurrent access.
func (a *Attribute[V]) ScanEntries(reader dal.PebbleReader, canonicalKey []byte) (*ScanResult[V], error) {
	value, err := a.Get(reader, canonicalKey)
	if err != nil {
		return nil, err
	}

	result := &ScanResult[V]{}

	if !reflect.ValueOf(value).IsNil() {
		result.LatestBase = value
		result.HasBase = true
	}

	return result, nil
}

// ComputeAllForPrefix computes the final value for all canonical keys sharing the
// given prefix. It performs a single forward scan using NewStreamingIter internally.
// This is more efficient than List + Get per key, as it uses one iterator
// scoped to just the prefix range instead of the entire attribute space.
// Thread-safe: allocates its own buffer for concurrent access.
func (a *Attribute[V]) ComputeAllForPrefix(reader dal.PebbleReader, canonicalPrefix []byte) ([]ComputedEntry[V], error) {
	si, err := a.NewStreamingIter(reader, canonicalPrefix)
	if err != nil {
		return nil, err
	}

	defer func() { _ = si.Close() }()

	var results []ComputedEntry[V]

	for si.Next() {
		results = append(results, si.Entry())
	}

	if err := si.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

// AttrTypeFromKey extracts the attribute type byte from a Pebble attribute key.
// Key layout: [0xF1][AttrType][CanonicalKey...] — AttrType is at fixed position 1.
// Returns (attrType, true) on success, or (0, false) if the key is too short.
func AttrTypeFromKey(pebbleKey []byte) (byte, bool) {
	if len(pebbleKey) <= 1+AttrTypeLen {
		return 0, false
	}

	return pebbleKey[1], true
}

// CanonicalKeyFromPebbleKey extracts the canonical key from a Pebble attribute key.
// Key layout: [0xF1][AttrType][CanonicalKey...] — canonical starts at offset 2.
// Returns nil if the key is too short.
func CanonicalKeyFromPebbleKey(pebbleKey []byte) []byte {
	if len(pebbleKey) <= 1+AttrTypeLen {
		return nil
	}

	return pebbleKey[2:]
}

// IncrementBytes increments a byte slice by 1 (treating as big-endian unsigned integer).
// Returns nil if all bytes are 0xFF (overflow).
func IncrementBytes(b []byte) []byte {
	result := make([]byte, len(b))
	copy(result, b)

	for i := len(result) - 1; i >= 0; i-- {
		result[i]++
		if result[i] != 0 {
			return result
		}
	}

	return nil
}
