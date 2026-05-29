package readstore

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

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

// PebbleAccountIterator iterates over unique account addresses for a single
// attribute type in the Pebble attributes zone. Keys have the format:
//
//	[0xF1][attrType][ledger\x00][address][sep][field...]
//
// The iterator deduplicates by address, emitting each account at most once.
// Use NewPebbleAccountIterator to merge V and M types for full enumeration.
type PebbleAccountIterator struct {
	iter   *pebble.Iterator
	prefix []byte // [0xF1][ledger\x00]

	current   []byte
	started   bool
	exhausted bool
}

// newSingleTypeAccountIterator creates a forward account iterator for one attribute type.
// With the type-prefixed key layout [0xF1][attrType][...], each type has its own
// contiguous key range — no need to skip transaction keys.
func newSingleTypeAccountIterator(reader dal.PebbleReader, attrType byte, ledgerID uint32, addrPrefix string) (*PebbleAccountIterator, error) {
	// Prefix for address extraction: [0xF1][attrType][ledgerID_BE_4B]
	prefix := make([]byte, 2+4)
	prefix[0] = dal.ZoneAttributes
	prefix[1] = attrType
	binary.BigEndian.PutUint32(prefix[2:], ledgerID)

	// Lower bound: [prefix][addrPrefix]
	lowerBound := make([]byte, len(prefix)+len(addrPrefix))
	copy(lowerBound, prefix)
	copy(lowerBound[len(prefix):], addrPrefix)

	upperBound := IncrementBytes(lowerBound)

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

// NewPebbleAccountIterator creates an iterator over all accounts in a ledger.
// It merges accounts from Volume and Metadata attribute types via OrIterator.
// The caller must close it when done.
func NewPebbleAccountIterator(reader dal.PebbleReader, ledgerID uint32) (EntityIterator, error) {
	return newMergedAccountIterator(reader, ledgerID, "")
}

// NewPebbleAccountPrefixIterator creates an iterator over accounts matching an
// address prefix. Used for compileAddressPrefix.
func NewPebbleAccountPrefixIterator(reader dal.PebbleReader, ledgerID uint32, addrPrefix string) (EntityIterator, error) {
	return newMergedAccountIterator(reader, ledgerID, addrPrefix)
}

// newMergedAccountIterator creates a forward account iterator that merges V and M types.
func newMergedAccountIterator(reader dal.PebbleReader, ledgerID uint32, addrPrefix string) (EntityIterator, error) {
	vIter, err := newSingleTypeAccountIterator(reader, dal.SubAttrVolume, ledgerID, addrPrefix)
	if err != nil {
		return nil, err
	}

	mIter, err := newSingleTypeAccountIterator(reader, dal.SubAttrMetadata, ledgerID, addrPrefix)
	if err != nil {
		vIter.Close()

		return nil, err
	}

	return NewOrIterator(vIter, mIter), nil
}

func (it *PebbleAccountIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		// SeekGE positions at the first key >= prefix within the iterator bounds.
		// Note: we use SeekGE (not SeekPrefixGE) because the main Pebble store
		// uses DefaultComparer whose Split returns len(key), making
		// SeekPrefixGE's implicit upper bound too restrictive.
		if !it.iter.SeekGE(it.prefix) {
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
	seekKey[n] = dal.CanonicalKeySepMetadata + 1 // past both separators

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

// newSingleTypeReverseAccountIterator creates a reverse account iterator for one attribute type.
func newSingleTypeReverseAccountIterator(reader dal.PebbleReader, attrType byte, ledgerID uint32) (*PebbleReverseAccountIterator, error) {
	prefix := make([]byte, 2+4)
	prefix[0] = dal.ZoneAttributes
	prefix[1] = attrType
	binary.BigEndian.PutUint32(prefix[2:], ledgerID)

	upperBound := IncrementBytes(prefix)

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
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

// NewPebbleReverseAccountIterator creates a reverse account iterator that merges
// V and M attribute types, yielding unique addresses in descending order.
func NewPebbleReverseAccountIterator(reader dal.PebbleReader, ledgerID uint32) (*ReverseOrIterator, error) {
	vIter, err := newSingleTypeReverseAccountIterator(reader, dal.SubAttrVolume, ledgerID)
	if err != nil {
		return nil, err
	}

	mIter, err := newSingleTypeReverseAccountIterator(reader, dal.SubAttrMetadata, ledgerID)
	if err != nil {
		vIter.Close()

		return nil, err
	}

	return NewReverseOrIterator(vIter, mIter), nil
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
//	[0xF1][T][ledger\x00\x02][txID(8B)]
//
// The iterator deduplicates by txID, emitting each transaction at most once.
type PebbleTxIterator struct {
	iter     *pebble.Iterator
	prefix   []byte // [0xF1][ledger\x00\x02]
	idOffset int    // offset where txID starts (= len(prefix))

	current   []byte
	started   bool
	exhausted bool
}

// NewPebbleTxIterator creates an iterator over all transactions in a ledger.
func NewPebbleTxIterator(reader dal.PebbleReader, ledgerID uint32) (*PebbleTxIterator, error) {
	prefix := txAttributeCode(ledgerID)
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
		if !it.iter.SeekGE(it.prefix) {
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
func NewPebbleReverseTxIterator(reader dal.PebbleReader, ledgerID uint32) (*PebbleReverseTxIterator, error) {
	prefix := txAttributeCode(ledgerID)
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
func NewLedgerLogIterator(reader dal.PebbleReader, kb *dal.KeyBuilder, ledgerID uint32) (*LedgerLogIterator, error) {
	prefix := LedgerLogPrefix(kb, ledgerID)

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
	iter       *pebble.Iterator
	lowerBound []byte // stored for SeekGE initial positioning
	idOffset   int

	current   []byte
	started   bool
	exhausted bool
}

// NewPebbleTxRangeIterator creates a bounded transaction iterator for range queries.
func NewPebbleTxRangeIterator(reader dal.PebbleReader, ledgerID uint32, lower, upper []byte) (*PebbleTxRangeIterator, error) {
	prefix := txAttributeCode(ledgerID)

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
		iter:       iter,
		lowerBound: lowerBound,
		idOffset:   len(prefix),
	}, nil
}

func (it *PebbleTxRangeIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		if !it.iter.SeekGE(it.lowerBound) {
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

	// Build seek key from stored lower bound prefix + target
	seekKey := make([]byte, it.idOffset+len(target))
	copy(seekKey, it.lowerBound[:min(it.idOffset, len(it.lowerBound))])
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

// txAttributeCode builds the Pebble key prefix for scanning transactions
// in a ledger within the attributes zone.
// Format: [0xF1][T][ledger\x00\x02].
func txAttributeCode(ledgerID uint32) []byte {
	prefix := make([]byte, 2+4+1)
	prefix[0] = dal.ZoneAttributes
	prefix[1] = dal.SubAttrTransaction
	binary.BigEndian.PutUint32(prefix[2:], ledgerID)
	prefix[6] = dal.CanonicalKeySepTransaction

	return prefix
}

// --- account address extraction ---

// extractAccountAddress extracts the account address from an attribute key.
// Key format: [0xF1][attrType][ledger\x00][address][sep][field...]
// where sep is CanonicalKeySepVolume (0x00) or CanonicalKeySepMetadata (0x01).
// The prefix parameter is [0xF1][attrType][ledger\x00].
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
