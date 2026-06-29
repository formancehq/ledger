package readstore

import (
	"github.com/cockroachdb/pebble/v2"
)

// DumpAuditIndexKeysForTest returns a copy of every audit-index key currently
// stored, in key order. Intended for tests and debugging (rebuild parity).
func (s *Store) DumpAuditIndexKeysForTest() [][]byte {
	lower := AuditIndexPrefix()
	upper := prefixUpperBound(lower)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil
	}
	defer func() { _ = iter.Close() }()

	var keys [][]byte
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		cp := make([]byte, len(k))
		copy(cp, k)
		keys = append(keys, cp)
	}
	return keys
}
