package readstore

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/pebble"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// PebbleAccountIterator iterates over unique account addresses stored in the
// Pebble attributes zone. Keys have the format:
//
//	[0xF1][ledger\x00][address\x00][sep][...][attrType][raftIndex(8B)]
//
// The iterator deduplicates by address, emitting each account at most once.
type PebbleAccountIterator struct {
	iter   *pebble.Iterator
	prefix []byte // [0xF1][ledger\x00]

	current   []byte
	started   bool
	exhausted bool
}

// NewPebbleAccountIterator creates an iterator over all accounts in a ledger.
// The caller must close it when done.
func NewPebbleAccountIterator(reader dal.PebbleReader, ledger string) (*PebbleAccountIterator, error) {
	// Prefix: [0xF1][ledger\x00]
	prefix := make([]byte, 1+len(ledger)+1)
	prefix[0] = dal.KeyPrefixAttributes
	copy(prefix[1:], ledger)
	prefix[1+len(ledger)] = 0x00

	upperBound := IncrementBytes(prefix)

	// LowerBound skips past the transaction sub-zone: tx keys start with
	// CanonicalKeySepTransaction (0x02) after the prefix. Account addresses
	// are ASCII (>= 0x20), so starting at [prefix][0x03] avoids a linear
	// scan through potentially millions of transaction attribute keys.
	lowerBound := make([]byte, len(prefix)+1)
	copy(lowerBound, prefix)
	lowerBound[len(prefix)] = dal.CanonicalKeySepTransaction + 1

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &PebbleAccountIterator{
		iter:   iter,
		prefix: prefix,
	}, nil
}

// NewPebbleAccountPrefixIterator creates an iterator over accounts matching an
// address prefix. Used for compileAddressPrefix.
func NewPebbleAccountPrefixIterator(reader dal.PebbleReader, ledger, addrPrefix string) (*PebbleAccountIterator, error) {
	// Lower: [0xF1][ledger\x00][addrPrefix]
	lower := make([]byte, 1+len(ledger)+1+len(addrPrefix))
	lower[0] = dal.KeyPrefixAttributes
	copy(lower[1:], ledger)
	lower[1+len(ledger)] = 0x00
	copy(lower[1+len(ledger)+1:], addrPrefix)

	upperBound := IncrementBytes(lower)

	// Prefix stays at [0xF1][ledger\x00] for extractAddress
	prefix := lower[:1+len(ledger)+1]

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &PebbleAccountIterator{
		iter:   iter,
		prefix: prefix,
	}, nil
}

func (it *PebbleAccountIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.First() {
			it.exhausted = true

			return false
		}

		addr := it.extractAddress(it.iter.Key())
		if addr != nil {
			it.current = copyBytes(addr)

			return true
		}
	}

	return it.advance()
}

func (it *PebbleAccountIterator) advance() bool {
	// Seek past all keys for the current address.
	// Attribute keys use separators 0x00 (volume) and 0x01 (metadata),
	// so [prefix][addr][0x02] is past all entries for addr.
	seekKey := make([]byte, len(it.prefix)+len(it.current)+1)
	n := copy(seekKey, it.prefix)
	n += copy(seekKey[n:], it.current)
	seekKey[n] = dal.CanonicalKeySepMetadata + 1 // 0x02, past both separators

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true

		return false
	}

	// Skip to next valid address, deduplicating
	for it.iter.Valid() {
		addr := it.extractAddress(it.iter.Key())
		if addr != nil && !bytes.Equal(addr, it.current) {
			it.current = copyBytes(addr)

			return true
		}

		if !it.iter.Next() {
			break
		}
	}

	it.exhausted = true

	return false
}

func (it *PebbleAccountIterator) Current() []byte {
	return it.current
}

func (it *PebbleAccountIterator) SeekGE(target []byte) bool {
	if it.exhausted {
		return false
	}

	it.started = true

	seekKey := make([]byte, len(it.prefix)+len(target))
	copy(seekKey, it.prefix)
	copy(seekKey[len(it.prefix):], target)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true

		return false
	}

	for it.iter.Valid() {
		addr := it.extractAddress(it.iter.Key())
		if addr != nil && compareEntities(addr, target) >= 0 {
			it.current = copyBytes(addr)

			return true
		}

		if !it.iter.Next() {
			break
		}
	}

	it.exhausted = true

	return false
}

func (it *PebbleAccountIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

// extractAddress extracts the account address from an attribute key.
// Key format: [0xF1][ledger\x00][address][sep][field][attrType][raftIndex]
// where sep is 0x00 (volume) or 0x01 (metadata).
// Returns the address bytes, or nil if the key doesn't have the expected format.
func (it *PebbleAccountIterator) extractAddress(key []byte) []byte {
	return extractAccountAddress(key, it.prefix)
}

// PebbleReversAccountIterator iterates over unique account addresses in
// descending order from the Pebble attributes zone.
type PebbleReverseAccountIterator struct {
	iter   *pebble.Iterator
	prefix []byte // [0xF1][ledger\x00]

	current   []byte
	started   bool
	exhausted bool
}

// NewPebbleReverseAccountIterator creates a reverse account iterator.
func NewPebbleReverseAccountIterator(reader dal.PebbleReader, ledger string) (*PebbleReverseAccountIterator, error) {
	prefix := make([]byte, 1+len(ledger)+1)
	prefix[0] = dal.KeyPrefixAttributes
	copy(prefix[1:], ledger)
	prefix[1+len(ledger)] = 0x00

	upperBound := IncrementBytes(prefix)

	// Skip past the transaction sub-zone (same rationale as PebbleAccountIterator).
	lowerBound := make([]byte, len(prefix)+1)
	copy(lowerBound, prefix)
	lowerBound[len(prefix)] = dal.CanonicalKeySepTransaction + 1

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &PebbleReverseAccountIterator{
		iter:   iter,
		prefix: prefix,
	}, nil
}

func (it *PebbleReverseAccountIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.Last() {
			it.exhausted = true

			return false
		}

		addr := it.extractAddress(it.iter.Key())
		if addr != nil {
			it.current = copyBytes(addr)

			return true
		}

		return it.prevAddress()
	}

	return it.prevAddress()
}

func (it *PebbleReverseAccountIterator) prevAddress() bool {
	// Seek to the start of the current address to skip all its keys:
	// SeekLT([prefix][currentAddr]) positions before any key for currentAddr
	seekKey := make([]byte, len(it.prefix)+len(it.current))
	copy(seekKey, it.prefix)
	copy(seekKey[len(it.prefix):], it.current)

	if !it.iter.SeekLT(seekKey) {
		it.exhausted = true

		return false
	}

	for it.iter.Valid() {
		addr := it.extractAddress(it.iter.Key())
		if addr != nil {
			it.current = copyBytes(addr)

			return true
		}

		if !it.iter.Prev() {
			break
		}
	}

	it.exhausted = true

	return false
}

func (it *PebbleReverseAccountIterator) Current() []byte {
	return it.current
}

func (it *PebbleReverseAccountIterator) SeekLE(target []byte) bool {
	if it.exhausted {
		return false
	}

	it.started = true

	// Seek to first key > all entries for target, then step back.
	// [prefix][target][0x02] is past both separators (0x00 volume, 0x01 metadata).
	seekKey := make([]byte, len(it.prefix)+len(target)+1)
	n := copy(seekKey, it.prefix)
	n += copy(seekKey[n:], target)
	seekKey[n] = dal.CanonicalKeySepMetadata + 1

	// Position at first key > seekKey, then go back
	if it.iter.SeekGE(seekKey) {
		// We found something >= seekKey. The current key might be for target or past it.
		// Go to last key for target by stepping back from the next address.
		addr := it.extractAddress(it.iter.Key())
		if addr != nil && compareEntities(addr, target) <= 0 {
			it.current = copyBytes(addr)

			return true
		}

		// Past target, step back
		if !it.iter.Prev() {
			it.exhausted = true

			return false
		}
	} else if !it.iter.Last() {
		// Past end, go to last
		it.exhausted = true

		return false
	}

	for it.iter.Valid() {
		addr := it.extractAddress(it.iter.Key())
		if addr != nil && compareEntities(addr, target) <= 0 {
			it.current = copyBytes(addr)

			return true
		}

		if !it.iter.Prev() {
			break
		}
	}

	it.exhausted = true

	return false
}

func (it *PebbleReverseAccountIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

func (it *PebbleReverseAccountIterator) extractAddress(key []byte) []byte {
	return extractAccountAddress(key, it.prefix)
}

// PebbleTxIterator iterates over unique transaction IDs stored in the Pebble
// attributes zone. Keys have the format:
//
//	[0xF1][ledger\x00\x02][txID(8B)]['T'][raftIndex(8B)]
//
// The iterator deduplicates by txID, emitting each transaction at most once
// (there may be multiple raft index entries per transaction).
type PebbleTxIterator struct {
	iter     *pebble.Iterator
	prefix   []byte // [0xF1][ledger\x00\x02]
	idOffset int    // offset where txID starts (= len(prefix))

	current   []byte
	started   bool
	exhausted bool
}

// NewPebbleTxIterator creates an iterator over all transactions in a ledger.
func NewPebbleTxIterator(reader dal.PebbleReader, ledger string) (*PebbleTxIterator, error) {
	prefix := txAttributePrefix(ledger)
	upperBound := IncrementBytes(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &PebbleTxIterator{
		iter:     iter,
		prefix:   prefix,
		idOffset: len(prefix),
	}, nil
}

func (it *PebbleTxIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.First() {
			it.exhausted = true

			return false
		}

		txID := it.extractTxID(it.iter.Key())
		if txID != nil {
			it.current = copyBytes(txID)

			return true
		}
	}

	return it.advanceToNextTx()
}

func (it *PebbleTxIterator) advanceToNextTx() bool {
	// Skip past all byLog entries for current txID:
	// Seek to [prefix][currentTxID+1 as 8B]
	nextTxID := incrementUint64Bytes(it.current)
	seekKey := make([]byte, len(it.prefix)+8)
	copy(seekKey, it.prefix)
	copy(seekKey[len(it.prefix):], nextTxID)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true

		return false
	}

	txID := it.extractTxID(it.iter.Key())
	if txID != nil {
		it.current = copyBytes(txID)

		return true
	}

	it.exhausted = true

	return false
}

func (it *PebbleTxIterator) Current() []byte {
	return it.current
}

func (it *PebbleTxIterator) SeekGE(target []byte) bool {
	if it.exhausted {
		return false
	}

	it.started = true

	seekKey := make([]byte, len(it.prefix)+len(target))
	copy(seekKey, it.prefix)
	copy(seekKey[len(it.prefix):], target)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true

		return false
	}

	txID := it.extractTxID(it.iter.Key())
	if txID != nil && compareEntities(txID, target) >= 0 {
		it.current = copyBytes(txID)

		return true
	}

	it.exhausted = true

	return false
}

func (it *PebbleTxIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

func (it *PebbleTxIterator) extractTxID(key []byte) []byte {
	if len(key) < it.idOffset+8 {
		return nil
	}

	return key[it.idOffset : it.idOffset+8]
}

// PebbleReverseTxIterator iterates over unique transaction IDs in descending order.
type PebbleReverseTxIterator struct {
	iter     *pebble.Iterator
	prefix   []byte
	idOffset int

	current   []byte
	started   bool
	exhausted bool
}

// NewPebbleReverseTxIterator creates a reverse transaction iterator.
func NewPebbleReverseTxIterator(reader dal.PebbleReader, ledger string) (*PebbleReverseTxIterator, error) {
	prefix := txAttributePrefix(ledger)
	upperBound := IncrementBytes(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &PebbleReverseTxIterator{
		iter:     iter,
		prefix:   prefix,
		idOffset: len(prefix),
	}, nil
}

func (it *PebbleReverseTxIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.Last() {
			it.exhausted = true

			return false
		}

		txID := it.extractTxID(it.iter.Key())
		if txID != nil {
			it.current = copyBytes(txID)

			return true
		}

		return it.prevTx()
	}

	return it.prevTx()
}

func (it *PebbleReverseTxIterator) prevTx() bool {
	// Seek before the first byLog entry for the current txID:
	// SeekLT([prefix][currentTxID])
	seekKey := make([]byte, len(it.prefix)+8)
	copy(seekKey, it.prefix)
	copy(seekKey[len(it.prefix):], it.current)

	if !it.iter.SeekLT(seekKey) {
		it.exhausted = true

		return false
	}

	txID := it.extractTxID(it.iter.Key())
	if txID != nil {
		it.current = copyBytes(txID)

		return true
	}

	it.exhausted = true

	return false
}

func (it *PebbleReverseTxIterator) Current() []byte {
	return it.current
}

func (it *PebbleReverseTxIterator) SeekLE(target []byte) bool {
	if it.exhausted {
		return false
	}

	it.started = true

	// Seek to the last byLog entry for target txID:
	// SeekGE([prefix][target+1]) then Prev(), or Last() if past end
	nextTarget := incrementUint64Bytes(target)
	seekKey := make([]byte, len(it.prefix)+8)
	copy(seekKey, it.prefix)
	copy(seekKey[len(it.prefix):], nextTarget)

	if it.iter.SeekGE(seekKey) {
		if !it.iter.Prev() {
			it.exhausted = true

			return false
		}
	} else {
		if !it.iter.Last() {
			it.exhausted = true

			return false
		}
	}

	for it.iter.Valid() {
		txID := it.extractTxID(it.iter.Key())
		if txID != nil && compareEntities(txID, target) <= 0 {
			it.current = copyBytes(txID)

			return true
		}

		if !it.iter.Prev() {
			break
		}
	}

	it.exhausted = true

	return false
}

func (it *PebbleReverseTxIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

func (it *PebbleReverseTxIterator) extractTxID(key []byte) []byte {
	if len(key) < it.idOffset+8 {
		return nil
	}

	return key[it.idOffset : it.idOffset+8]
}

// LedgerLogIterator iterates over log IDs from the read index (Pebble).
// Keys: [0x09][ledger\x00][logID_BE(8B)].
type LedgerLogIterator struct {
	inner *PrefixIterator
}

// NewLedgerLogIterator creates a forward iterator over logs in a ledger.
func NewLedgerLogIterator(reader dal.PebbleReader, kb *dal.KeyBuilder, ledger string) (*LedgerLogIterator, error) {
	prefix := LedgerLogPrefix(kb, ledger)

	inner, err := NewPrefixIterator(reader, prefix, len(prefix), 8)
	if err != nil {
		return nil, err
	}

	return &LedgerLogIterator{inner: inner}, nil
}

func (it *LedgerLogIterator) Next() bool                { return it.inner.Next() }
func (it *LedgerLogIterator) Current() []byte           { return it.inner.Current() }
func (it *LedgerLogIterator) SeekGE(target []byte) bool { return it.inner.SeekGE(target) }
func (it *LedgerLogIterator) Close()                    { it.inner.Close() }

// PebbleTxRangeIterator iterates over transaction IDs within a [min, max) range.
// Used for compileTxIDCondition range scans.
type PebbleTxRangeIterator struct {
	iter     *pebble.Iterator
	idOffset int

	current   []byte
	started   bool
	exhausted bool
}

// NewPebbleTxRangeIterator creates a bounded transaction iterator for range queries.
func NewPebbleTxRangeIterator(reader dal.PebbleReader, ledger string, lower, upper []byte) (*PebbleTxRangeIterator, error) {
	prefix := txAttributePrefix(ledger)

	lowerBound := make([]byte, len(prefix)+len(lower))
	copy(lowerBound, prefix)
	copy(lowerBound[len(prefix):], lower)

	var upperBound []byte
	if upper != nil {
		upperBound = make([]byte, len(prefix)+len(upper))
		copy(upperBound, prefix)
		copy(upperBound[len(prefix):], upper)
	} else {
		upperBound = IncrementBytes(prefix)
	}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &PebbleTxRangeIterator{
		iter:     iter,
		idOffset: len(prefix),
	}, nil
}

func (it *PebbleTxRangeIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.First() {
			it.exhausted = true

			return false
		}

		txID := it.extractTxID(it.iter.Key())
		if txID != nil {
			it.current = copyBytes(txID)

			return true
		}
	}

	// Skip byLog entries for current txID
	nextTxID := incrementUint64Bytes(it.current)
	seekKey := make([]byte, it.idOffset+8)
	// Reconstruct the prefix from the current Pebble key.
	if k := it.iter.Key(); len(k) >= it.idOffset {
		copy(seekKey, k[:it.idOffset])
	}

	copy(seekKey[it.idOffset:], nextTxID)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true

		return false
	}

	txID := it.extractTxID(it.iter.Key())
	if txID != nil {
		it.current = copyBytes(txID)

		return true
	}

	it.exhausted = true

	return false
}

func (it *PebbleTxRangeIterator) Current() []byte { return it.current }

func (it *PebbleTxRangeIterator) SeekGE(target []byte) bool {
	if it.exhausted {
		return false
	}

	it.started = true

	if !it.iter.First() {
		it.exhausted = true

		return false
	}

	// Build seek key from current iter key prefix + target
	key := it.iter.Key()
	seekKey := make([]byte, it.idOffset+len(target))

	if len(key) >= it.idOffset {
		copy(seekKey, key[:it.idOffset])
	}

	copy(seekKey[it.idOffset:], target)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true

		return false
	}

	txID := it.extractTxID(it.iter.Key())
	if txID != nil && compareEntities(txID, target) >= 0 {
		it.current = copyBytes(txID)

		return true
	}

	it.exhausted = true

	return false
}

func (it *PebbleTxRangeIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

func (it *PebbleTxRangeIterator) extractTxID(key []byte) []byte {
	if len(key) < it.idOffset+8 {
		return nil
	}

	return key[it.idOffset : it.idOffset+8]
}

// --- transaction prefix helper ---

// txAttributePrefix builds the Pebble key prefix for scanning transactions
// in a ledger within the attributes zone.
// Format: [0xF1][ledger\x00\x02].
func txAttributePrefix(ledger string) []byte {
	prefix := make([]byte, 1+len(ledger)+1+1)
	prefix[0] = dal.KeyPrefixAttributes
	copy(prefix[1:], ledger)
	prefix[1+len(ledger)] = 0x00
	prefix[1+len(ledger)+1] = dal.CanonicalKeySepTransaction

	return prefix
}

// --- account address extraction ---

// extractAccountAddress extracts the account address from an attribute key.
// Key format after prefix: [address][sep][field][attrType][raftIndex]
// where sep is CanonicalKeySepVolume (0x00) or CanonicalKeySepMetadata (0x01).
func extractAccountAddress(key, prefix []byte) []byte {
	if len(key) <= len(prefix) {
		return nil
	}

	suffix := key[len(prefix):]

	// Skip transaction keys: they start with CanonicalKeySepTransaction (0x02)
	// after the ledger prefix.
	if len(suffix) > 0 && suffix[0] == dal.CanonicalKeySepTransaction {
		return nil
	}

	// Find the first canonical key separator (0x00 for volume, 0x01 for metadata).
	idx0 := bytes.IndexByte(suffix, dal.CanonicalKeySepVolume)
	idx1 := bytes.IndexByte(suffix, dal.CanonicalKeySepMetadata)

	idx := idx0
	if idx < 0 || (idx1 >= 0 && idx1 < idx) {
		idx = idx1
	}

	if idx <= 0 {
		return nil
	}

	return suffix[:idx]
}

// --- helpers ---

func copyBytes(b []byte) []byte {
	cp := make([]byte, len(b))
	copy(cp, b)

	return cp
}

func incrementUint64Bytes(b []byte) []byte {
	if len(b) != 8 {
		return b
	}

	val := binary.BigEndian.Uint64(b)
	val++

	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, val)

	return result
}
