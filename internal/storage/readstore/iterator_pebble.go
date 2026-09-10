package readstore

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

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
	floor     seekFloor
}

// newSingleTypeAccountIterator creates a forward account iterator for one attribute type.
// With the type-prefixed key layout [0xF1][attrType][...], each type has its own
// contiguous key range — no need to skip transaction keys.
func newSingleTypeAccountIterator(reader dal.PebbleReader, attrType byte, ledgerName string, addrPrefix string) (*PebbleAccountIterator, error) {
	// Prefix for address extraction: [0xF1][attrType][ledgerName padded 64B]
	prefix := make([]byte, 2+dal.LedgerNameFixedSize)
	prefix[0] = dal.ZoneAttributes
	prefix[1] = attrType
	copy(prefix[2:], ledgerName)

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
func NewPebbleAccountIterator(reader dal.PebbleReader, ledgerName string) (EntityIterator, error) {
	return newMergedAccountIterator(reader, ledgerName, "")
}

// NewPebbleAccountPrefixIterator creates an iterator over accounts matching an
// address prefix. Used for compileAddressPrefix.
func NewPebbleAccountPrefixIterator(reader dal.PebbleReader, ledgerName string, addrPrefix string) (EntityIterator, error) {
	return newMergedAccountIterator(reader, ledgerName, addrPrefix)
}

// newMergedAccountIterator creates a forward account iterator that merges V and M types.
func newMergedAccountIterator(reader dal.PebbleReader, ledgerName string, addrPrefix string) (EntityIterator, error) {
	vIter, err := newSingleTypeAccountIterator(reader, dal.SubAttrVolume, ledgerName, addrPrefix)
	if err != nil {
		return nil, err
	}

	mIter, err := newSingleTypeAccountIterator(reader, dal.SubAttrMetadata, ledgerName, addrPrefix)
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
		// Seek positions at the first key >= prefix within the iterator bounds.
		// Note: we use Seek (not SeekPrefixGE) because the main Pebble store
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

func (it *PebbleAccountIterator) Seek(target []byte) bool {
	// A prior failed seek at or below target proves this one empty too.
	if it.floor.covers(target) {
		it.exhausted = true

		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion still finds entities (the body re-seeks from target).
	it.exhausted = false

	it.started = true

	seekKey := make([]byte, len(it.prefix)+len(target))
	copy(seekKey, it.prefix)
	copy(seekKey[len(it.prefix):], target)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true
		it.floor.fail(target, it.iter.Error())

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
	it.floor.fail(target, it.iter.Error())

	return false
}

func (it *PebbleAccountIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
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
	ceil      seekCeil
}

// newSingleTypeReverseAccountIterator creates a reverse account iterator for one attribute type.
func newSingleTypeReverseAccountIterator(reader dal.PebbleReader, attrType byte, ledgerName string, addrPrefix string) (*PebbleReverseAccountIterator, error) {
	prefix := make([]byte, 2+dal.LedgerNameFixedSize)
	prefix[0] = dal.ZoneAttributes
	prefix[1] = attrType
	copy(prefix[2:], ledgerName)

	// Bounds mirror newSingleTypeAccountIterator: an empty addrPrefix scans
	// the whole ledger, a non-empty one scans that address range.
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

	return &PebbleReverseAccountIterator{
		iter:   iter,
		prefix: prefix,
	}, nil
}

// NewPebbleReverseAccountIterator creates a reverse account iterator that merges
// V and M attribute types, yielding unique addresses in descending order.
func NewPebbleReverseAccountIterator(reader dal.PebbleReader, ledgerName string) (*OrIterator[Desc], error) {
	return NewPebbleReverseAccountPrefixIterator(reader, ledgerName, "")
}

// NewPebbleReverseAccountPrefixIterator is the descending twin of
// NewPebbleAccountPrefixIterator: unique addresses under addrPrefix, high to
// low. Accounts are entity-ordered in the attributes zone, so an address
// prefix match streams in both directions (EN-1966).
func NewPebbleReverseAccountPrefixIterator(reader dal.PebbleReader, ledgerName string, addrPrefix string) (*OrIterator[Desc], error) {
	vIter, err := newSingleTypeReverseAccountIterator(reader, dal.SubAttrVolume, ledgerName, addrPrefix)
	if err != nil {
		return nil, err
	}

	mIter, err := newSingleTypeReverseAccountIterator(reader, dal.SubAttrMetadata, ledgerName, addrPrefix)
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

func (it *PebbleReverseAccountIterator) Seek(target []byte) bool {
	// A prior failed seek at or above target proves this one empty too.
	if it.ceil.covers(target) {
		it.exhausted = true

		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion still finds entities (the body re-seeks from target).
	it.exhausted = false

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
			it.ceil.fail(target, it.iter.Error())

			return false
		}
	} else if !it.iter.Last() {
		// Past end, go to last
		it.exhausted = true
		it.ceil.fail(target, it.iter.Error())

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
	it.ceil.fail(target, it.iter.Error())

	return false
}

func (it *PebbleReverseAccountIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
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
	floor     seekFloor
}

// NewPebbleTxIterator creates an iterator over all transactions in a ledger.
func NewPebbleTxIterator(reader dal.PebbleReader, ledgerName string) (*PebbleTxIterator, error) {
	prefix := txAttributeCode(ledgerName)
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

func (it *PebbleTxIterator) Seek(target []byte) bool {
	// A prior failed seek at or below target proves this one empty too.
	if it.floor.covers(target) {
		it.exhausted = true

		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion still finds entities (the body re-seeks from target).
	it.exhausted = false

	it.started = true

	seekKey := make([]byte, len(it.prefix)+len(target))
	copy(seekKey, it.prefix)
	copy(seekKey[len(it.prefix):], target)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true
		it.floor.fail(target, it.iter.Error())

		return false
	}

	txID := it.extractTxID(it.iter.Key())
	if txID != nil && compareEntities(txID, target) >= 0 {
		it.current = copyBytes(txID)

		return true
	}

	it.exhausted = true
	it.floor.fail(target, it.iter.Error())

	return false
}

func (it *PebbleTxIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
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
	ceil      seekCeil
}

// NewPebbleReverseTxIterator creates a reverse transaction iterator.
func NewPebbleReverseTxIterator(reader dal.PebbleReader, ledgerName string) (*PebbleReverseTxIterator, error) {
	prefix := txAttributeCode(ledgerName)
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

// NewPebbleReverseTxRangeIterator is NewPebbleReverseTxIterator restricted to
// [lower, upper) on the transaction id, mirroring NewPebbleTxRangeIterator's
// bound construction. A nil bound means "open on that side". The traversal
// logic is unchanged: Last/Prev/SeekLT all respect the Pebble bounds, so the
// descending id-range scan streams exactly like the ascending one (EN-1966).
func NewPebbleReverseTxRangeIterator(reader dal.PebbleReader, ledgerName string, lower, upper []byte) (*PebbleReverseTxIterator, error) {
	prefix := txAttributeCode(ledgerName)

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

func (it *PebbleReverseTxIterator) Seek(target []byte) bool {
	// A prior failed seek at or above target proves this one empty too.
	if it.ceil.covers(target) {
		it.exhausted = true

		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion still finds entities (the body re-seeks from target).
	it.exhausted = false

	it.started = true

	// Seek to the last byLog entry for target txID:
	// Seek([prefix][target+1]) then Prev(), or Last() if past end.
	// An all-0xff target wraps the increment to zero, which would land the
	// probe on the FIRST key and mis-record an emptiness proof; every key
	// qualifies for that target, so position at the end of the range directly.
	var positioned bool
	if isMaxUint64Bytes(target) {
		positioned = it.iter.Last()
	} else {
		nextTarget := incrementUint64Bytes(target)
		seekKey := make([]byte, len(it.prefix)+8)
		copy(seekKey, it.prefix)
		copy(seekKey[len(it.prefix):], nextTarget)

		if it.iter.SeekGE(seekKey) {
			positioned = it.iter.Prev()
		} else {
			positioned = it.iter.Last()
		}
	}

	if !positioned {
		it.exhausted = true
		it.ceil.fail(target, it.iter.Error())

		return false
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
	it.ceil.fail(target, it.iter.Error())

	return false
}

func (it *PebbleReverseTxIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
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
func NewLedgerLogIterator(reader dal.PebbleReader, kb *dal.KeyBuilder, ledgerName string) (*LedgerLogIterator, error) {
	prefix := LedgerLogPrefix(kb, ledgerName)

	inner, err := NewPrefixIterator(reader, prefix, len(prefix), 8)
	if err != nil {
		return nil, err
	}

	return &LedgerLogIterator{inner: inner}, nil
}

func (it *LedgerLogIterator) Next() bool              { return it.inner.Next() }
func (it *LedgerLogIterator) Current() []byte         { return it.inner.Current() }
func (it *LedgerLogIterator) Seek(target []byte) bool { return it.inner.Seek(target) }
func (it *LedgerLogIterator) Err() error              { return it.inner.Err() }
func (it *LedgerLogIterator) Close()                  { it.inner.Close() }

// PebbleTxRangeIterator iterates over transaction IDs within a [min, max) range.
// Used for compileTxIDCondition range scans.
type PebbleTxRangeIterator struct {
	iter       *pebble.Iterator
	lowerBound []byte // stored for Seek initial positioning
	idOffset   int

	current   []byte
	started   bool
	exhausted bool
	floor     seekFloor
}

// NewReverseLedgerLogIterator is the descending twin of NewLedgerLogIterator:
// log ids under the ledger's log prefix, high to low. Logs are entity-ordered
// in the index, so the scan streams in both directions (EN-1966).
func NewReverseLedgerLogIterator(reader dal.PebbleReader, kb *dal.KeyBuilder, ledgerName string) (*ReversePrefixIterator, error) {
	prefix := LedgerLogPrefix(kb, ledgerName)

	return NewReversePrefixIterator(reader, prefix, len(prefix), 8)
}

// NewPebbleTxRangeIterator creates a bounded transaction iterator for range queries.
func NewPebbleTxRangeIterator(reader dal.PebbleReader, ledgerName string, lower, upper []byte) (*PebbleTxRangeIterator, error) {
	prefix := txAttributeCode(ledgerName)

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

func (it *PebbleTxRangeIterator) Seek(target []byte) bool {
	// A prior failed seek at or below target proves this one empty too.
	if it.floor.covers(target) {
		it.exhausted = true

		return false
	}

	// Absolute reposition: clear the exhausted latch so a re-seek after
	// exhaustion still finds entities (the body re-seeks from target).
	it.exhausted = false

	it.started = true

	// Build seek key from stored lower bound prefix + target
	seekKey := make([]byte, it.idOffset+len(target))
	copy(seekKey, it.lowerBound[:min(it.idOffset, len(it.lowerBound))])
	copy(seekKey[it.idOffset:], target)

	if !it.iter.SeekGE(seekKey) {
		it.exhausted = true
		it.floor.fail(target, it.iter.Error())

		return false
	}

	txID := it.extractTxID(it.iter.Key())
	if txID != nil && compareEntities(txID, target) >= 0 {
		it.current = copyBytes(txID)

		return true
	}

	it.exhausted = true
	it.floor.fail(target, it.iter.Error())

	return false
}

func (it *PebbleTxRangeIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
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
// Format: [0xF1][T][ledgerName padded 64B][0x02].
func txAttributeCode(ledgerName string) []byte {
	prefix := make([]byte, 2+dal.LedgerNameFixedSize+1)
	prefix[0] = dal.ZoneAttributes
	prefix[1] = dal.SubAttrTransaction
	copy(prefix[2:2+dal.LedgerNameFixedSize], ledgerName)
	prefix[2+dal.LedgerNameFixedSize] = dal.CanonicalKeySepTransaction

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

// isMaxUint64Bytes reports whether b is the 8-byte encoding of MaxUint64 —
// the one value incrementUint64Bytes wraps to zero on.
func isMaxUint64Bytes(b []byte) bool {
	return len(b) == 8 && binary.BigEndian.Uint64(b) == math.MaxUint64
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

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *PebbleAccountIterator) Direction() (d Asc) { return }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *PebbleTxIterator) Direction() (d Asc) { return }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *PebbleTxRangeIterator) Direction() (d Asc) { return }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *LedgerLogIterator) Direction() (d Asc) { return }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *PebbleReverseAccountIterator) Direction() (d Desc) { return }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *PebbleReverseTxIterator) Direction() (d Desc) { return }
