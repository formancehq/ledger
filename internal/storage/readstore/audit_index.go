package readstore

import (
	"encoding/binary"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadAuditProgress returns the last indexed audit sequence (0 if unset).
func (s *Store) ReadAuditProgress() (uint64, error) {
	return auditCursor.Read(s.db)
}

// WriteAuditProgress persists the audit indexing cursor in the batch.
func (s *Store) WriteAuditProgress(batch *dal.WriteSession, sequence uint64) error {
	return auditCursor.Write(batch, sequence)
}

// DropAuditIndexInBatch stages deletion of every audit-index key (but NOT the
// cursor) into batch so a rebuild can repopulate from scratch. The caller owns
// the commit, allowing the drop to be made atomic with a cursor reset.
func (s *Store) DropAuditIndexInBatch(batch *dal.WriteSession) error {
	start := AuditIndexPrefix()
	if err := batch.DeleteRange(start, prefixUpperBound(start), nil); err != nil {
		return fmt.Errorf("dropping audit index: %w", err)
	}

	return nil
}

// DropAuditIndex removes every audit-index key (but NOT the cursor) so a
// rebuild can repopulate from scratch.
func (s *Store) DropAuditIndex() error {
	batch := s.NewBatch()
	defer func() { _ = batch.Cancel() }()
	if err := s.DropAuditIndexInBatch(batch); err != nil {
		return err
	}

	return batch.Commit()
}

// prefixUpperBound returns the smallest key strictly greater than every key
// that has prefix as a prefix (the standard exclusive bound for a prefix scan).
// Returns nil when prefix is all 0xFF, meaning "no upper bound".
func prefixUpperBound(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i, v := range slices.Backward(end) {
		if v != 0xFF {
			end[i]++

			return end[:i+1]
		}
	}

	return nil
}

// auditSeqsForPrefix iterates the half-open range [lower, upper) and extracts
// the trailing 8-byte big-endian audit sequence from each key, deduplicated.
//
// A single audit entry can produce several keys carrying the same sequence
// within one scan range — match-any fields emit one key per value (e.g. one
// AuditFieldLogSeq key per item, or one AuditFieldLedger key per ledger). The
// caller wants each matching entry once, so duplicates are collapsed. They are
// not necessarily adjacent (keys sort by value then seq, so the same seq can
// appear at different value positions), hence a seen-set rather than a
// previous-value comparison. Results are sorted ascending by sequence.
//
// exactLen, when non-zero, restricts matching to keys of exactly that byte
// length. String-valued fields are indexed as [field][value\x00][seq BE8] and
// scanned by the prefix [field][value\x00]; without the length guard a lookup
// for "alice" would also match a value indexed as "alice\x00evil" (whose key
// shares the prefix). Fixed-width fields (uint64, byte) pass exactLen=0 since
// their value segment cannot be a prefix of a longer value.
func (s *Store) auditSeqsForPrefix(lower, upper []byte, exactLen int) ([]uint64, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("creating audit index iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var seqs []uint64
	seen := make(map[uint64]struct{})
	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) < 8 {
			return nil, fmt.Errorf("audit index key too short: %d", len(k))
		}
		if exactLen != 0 && len(k) != exactLen {
			continue
		}
		seq := binary.BigEndian.Uint64(k[len(k)-8:])
		if _, ok := seen[seq]; ok {
			continue
		}
		seen[seq] = struct{}{}
		seqs = append(seqs, seq)
	}
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating audit index: %w", err)
	}

	// Keys sort by value then seq, so a range/match-any scan can surface
	// sequences out of global order (e.g. two ledgers, or a [lo,hi] range).
	// Callers rely on ascending seq order for cursor/reverse pagination and
	// for set intersection/union, so normalize here.
	slices.Sort(seqs)

	return seqs, nil
}

// AuditSeqsByString returns audit sequences indexed under a string field for an
// exact value (equality match).
//
// Matching is a prefix scan over [field][value\x00]. Because an indexed value
// may itself contain a NUL byte (a caller.subject is an arbitrary auth-server
// string), the prefix alone is ambiguous — "alice" would also match a value
// stored as "alice\x00evil". The exact-length guard (prefix + 8-byte seq)
// rejects any key longer than a single [prefix][seq] entry, so only the true
// equality matches survive.
func (s *Store) AuditSeqsByString(field byte, value string) ([]uint64, error) {
	kb := dal.NewKeyBuilder()
	lower := kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalAuditIndex).
		PutByte(field).
		PutStringNull(value).
		Build()

	return s.auditSeqsForPrefix(lower, prefixUpperBound(lower), len(lower)+8)
}

// AuditSeqsByOutcome returns audit sequences for success (true) or failure (false).
func (s *Store) AuditSeqsByOutcome(success bool) ([]uint64, error) {
	var b byte
	if success {
		b = 1
	}
	kb := dal.NewKeyBuilder()
	lower := kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalAuditIndex).
		PutByte(AuditFieldOutcome).
		PutByte(b).
		Build()

	return s.auditSeqsForPrefix(lower, prefixUpperBound(lower), 0)
}

// AuditSeqsByUint64Range returns audit sequences for a numeric field whose value
// falls in [lo, hi] inclusive.
func (s *Store) AuditSeqsByUint64Range(field byte, lo, hi uint64) ([]uint64, error) {
	kb := dal.NewKeyBuilder()
	lower := kb.Reset().
		PutByte(PrefixInternal).
		PutByte(SubInternalAuditIndex).
		PutByte(field).
		PutUint64(lo).
		Build()

	ukb := dal.NewKeyBuilder()
	var upper []byte
	if hi == ^uint64(0) {
		fieldPrefix := ukb.Reset().
			PutByte(PrefixInternal).
			PutByte(SubInternalAuditIndex).
			PutByte(field).
			Build()
		upper = prefixUpperBound(fieldPrefix)
	} else {
		upper = ukb.Reset().
			PutByte(PrefixInternal).
			PutByte(SubInternalAuditIndex).
			PutByte(field).
			PutUint64(hi + 1).
			Build()
	}

	return s.auditSeqsForPrefix(lower, upper, 0)
}
