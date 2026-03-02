package readstore

import "bytes"

// EntityIterator iterates over sorted entity IDs (account addresses or
// transaction IDs as raw bytes). All iterators produce entities in ascending
// byte order.
type EntityIterator interface {
	// Next advances to the next entity. Returns false when exhausted.
	Next() bool

	// Current returns the current entity ID. The returned slice is only
	// valid until the next call to Next or SeekGE.
	Current() []byte

	// SeekGE positions the iterator at the first entity >= target.
	// Returns false if no such entity exists.
	SeekGE(target []byte) bool

	// Close releases resources held by this iterator.
	Close()
}

// compareEntities compares two entity IDs in byte order.
// Returns -1, 0, or 1.
func compareEntities(a, b []byte) int {
	return bytes.Compare(a, b)
}

// HasPrefix returns true if key starts with prefix.
func HasPrefix(key, prefix []byte) bool {
	return len(key) >= len(prefix) && bytes.Equal(key[:len(prefix)], prefix)
}
