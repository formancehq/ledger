package readstore

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/kv"
)

// compareEntities compares two entity IDs in byte order.
// Returns -1, 0, or 1.
func compareEntities(a, b []byte) int {
	return bytes.Compare(a, b)
}

// AccountIterator iterates over unique account addresses for a single
// attribute type in the main store's attributes zone. Keys have the format:
//
//	[ZoneAttributes][attrType][ledgerName padded 64B][address][field...]
//
// The iterator deduplicates by address, emitting each account at most once.
// Use NewAccountIterator to merge V and M types for full enumeration.
type AccountIterator struct {
	iter   *kv.Iterator
	prefix []byte // [ZoneAttributes][attrType][ledgerName padded 64B]

	current   []byte
	started   bool
	exhausted bool
	floor     seekFloor
}

// newSingleTypeAccountIterator creates a forward account iterator for one attribute type.
// With the type-prefixed key layout [0xF1][attrType][...], each type has its own
// contiguous key range — no need to skip transaction keys.
func newSingleTypeAccountIterator(reader dal.KVReader, attrType byte, ledgerName string, addrPrefix string) (*AccountIterator, error) {
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

	iter, err := reader.NewIter(&kv.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &AccountIterator{
		iter:   iter,
		prefix: prefix,
	}, nil
}

// NewAccountIterator creates an iterator over all accounts in a ledger.
// It merges accounts from Volume and Metadata attribute types via OrIterator.
// The caller must close it when done.
func NewAccountIterator(reader dal.KVReader, ledgerName string) (EntityIterator, error) {
	return newMergedAccountIterator(reader, ledgerName, "")
}

// NewAccountPrefixIterator creates an iterator over accounts matching an
// address prefix. Used for compileAddressPrefix.
func NewAccountPrefixIterator(reader dal.KVReader, ledgerName string, addrPrefix string) (EntityIterator, error) {
	return newMergedAccountIterator(reader, ledgerName, addrPrefix)
}

// newMergedAccountIterator creates a forward account iterator that merges V and M types.
func newMergedAccountIterator(reader dal.KVReader, ledgerName string, addrPrefix string) (EntityIterator, error) {
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

func (it *AccountIterator) Next() bool {
	if it.exhausted {
		return false
	}

	if !it.started {
		it.started = true
		// Seek positions at the first key >= prefix within the iterator bounds.
		// SeekGE respects the explicit iterator bounds for this attribute type.
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

func (it *AccountIterator) advance() bool {
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

func (it *AccountIterator) Current() []byte {
	return it.current
}

func (it *AccountIterator) Seek(target []byte) bool {
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

func (it *AccountIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
}

func (it *AccountIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

// extractAddress extracts the account address from an attribute key.
func (it *AccountIterator) extractAddress(key []byte) []byte {
	return extractAccountAddress(key, it.prefix)
}

// ReverseAccountIterator iterates over unique account addresses in
// descending order from the main store's attributes zone.
type ReverseAccountIterator struct {
	iter   *kv.Iterator
	prefix []byte // [ZoneAttributes][attrType][ledgerName padded 64B]

	current   []byte
	started   bool
	exhausted bool
	ceil      seekCeil
}

// newSingleTypeReverseAccountIterator creates a reverse account iterator for one attribute type.
func newSingleTypeReverseAccountIterator(reader dal.KVReader, attrType byte, ledgerName string, addrPrefix string) (*ReverseAccountIterator, error) {
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

	iter, err := reader.NewIter(&kv.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &ReverseAccountIterator{
		iter:   iter,
		prefix: prefix,
	}, nil
}

// NewReverseAccountIterator creates a reverse account iterator that merges
// V and M attribute types, yielding unique addresses in descending order.
func NewReverseAccountIterator(reader dal.KVReader, ledgerName string) (*OrIterator[Desc], error) {
	return NewReverseAccountPrefixIterator(reader, ledgerName, "")
}

// NewReverseAccountPrefixIterator is the descending twin of
// NewAccountPrefixIterator: unique addresses under addrPrefix, high to
// low. Accounts are entity-ordered in the attributes zone, so an address
// prefix match streams in both directions (EN-1966).
func NewReverseAccountPrefixIterator(reader dal.KVReader, ledgerName string, addrPrefix string) (*OrIterator[Desc], error) {
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

func (it *ReverseAccountIterator) Next() bool {
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

func (it *ReverseAccountIterator) prevAddress() bool {
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

func (it *ReverseAccountIterator) Current() []byte {
	return it.current
}

func (it *ReverseAccountIterator) Seek(target []byte) bool {
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

func (it *ReverseAccountIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
}

func (it *ReverseAccountIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

func (it *ReverseAccountIterator) extractAddress(key []byte) []byte {
	return extractAccountAddress(key, it.prefix)
}

// TxIterator iterates over unique transaction IDs stored in the main store's
// attributes zone. Keys have the format:
//
//	[ZoneAttributes][SubAttrTransaction][ledgerName padded 64B][separator][txID 8B]
//
// The iterator deduplicates by txID, emitting each transaction at most once.
type TxIterator struct {
	iter     *kv.Iterator
	prefix   []byte // transaction attribute prefix for one ledger
	idOffset int    // offset where txID starts (= len(prefix))

	current   []byte
	started   bool
	exhausted bool
	floor     seekFloor
}

// NewTxIterator creates an iterator over all transactions in a ledger.
func NewTxIterator(reader dal.KVReader, ledgerName string) (*TxIterator, error) {
	prefix := txAttributeCode(ledgerName)
	upperBound := IncrementBytes(prefix)

	iter, err := reader.NewIter(&kv.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &TxIterator{
		iter:     iter,
		prefix:   prefix,
		idOffset: len(prefix),
	}, nil
}

func (it *TxIterator) Next() bool {
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

func (it *TxIterator) advanceToNextTx() bool {
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

func (it *TxIterator) Current() []byte {
	return it.current
}

func (it *TxIterator) Seek(target []byte) bool {
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

func (it *TxIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
}

func (it *TxIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

func (it *TxIterator) extractTxID(key []byte) []byte {
	if len(key) < it.idOffset+8 {
		return nil
	}

	return key[it.idOffset : it.idOffset+8]
}

// ReverseTxIterator iterates over unique transaction IDs in descending order.
type ReverseTxIterator struct {
	iter     *kv.Iterator
	prefix   []byte
	idOffset int

	current   []byte
	started   bool
	exhausted bool
	ceil      seekCeil
}

// NewReverseTxIterator creates a reverse transaction iterator.
func NewReverseTxIterator(reader dal.KVReader, ledgerName string) (*ReverseTxIterator, error) {
	prefix := txAttributeCode(ledgerName)
	upperBound := IncrementBytes(prefix)

	iter, err := reader.NewIter(&kv.IterOptions{
		LowerBound: prefix,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &ReverseTxIterator{
		iter:     iter,
		prefix:   prefix,
		idOffset: len(prefix),
	}, nil
}

// NewReverseTxRangeIterator is NewReverseTxIterator restricted to
// [lower, upper) on the transaction id, mirroring NewTxRangeIterator's
// bound construction. A nil bound means "open on that side". The traversal
// logic is unchanged: Last/Prev/SeekLT all respect the iterator bounds, so the
// descending id-range scan streams exactly like the ascending one (EN-1966).
func NewReverseTxRangeIterator(reader dal.KVReader, ledgerName string, lower, upper []byte) (*ReverseTxIterator, error) {
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

	iter, err := reader.NewIter(&kv.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, err
	}

	return &ReverseTxIterator{
		iter:     iter,
		prefix:   prefix,
		idOffset: len(prefix),
	}, nil
}

func (it *ReverseTxIterator) Next() bool {
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

func (it *ReverseTxIterator) prevTx() bool {
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

func (it *ReverseTxIterator) Current() []byte {
	return it.current
}

func (it *ReverseTxIterator) Seek(target []byte) bool {
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

func (it *ReverseTxIterator) Err() error {
	if it.iter == nil {
		return nil
	}

	return it.iter.Error()
}

func (it *ReverseTxIterator) Close() {
	if it.iter != nil {
		_ = it.iter.Close()
	}
}

func (it *ReverseTxIterator) extractTxID(key []byte) []byte {
	if len(key) < it.idOffset+8 {
		return nil
	}

	return key[it.idOffset : it.idOffset+8]
}

// LedgerLogRangeIterator streams ledger-local log IDs in a half-open range.
// Keys: [0x09][ledger 64B][logID_BE(8B)], one key per log ID.
type LedgerLogRangeIterator struct {
	*BoundedEntityIterator
}

// NewLedgerLogRangeIterator creates a bounded forward iterator over log IDs.
// Nil bounds are open; lower is inclusive and upper is exclusive.
func NewLedgerLogRangeIterator(reader dal.KVReader, kb *dal.KeyBuilder, ledgerName string, lower, upper []byte) (*LedgerLogRangeIterator, error) {
	inner, err := NewBoundedEntityIterator(reader, LedgerLogPrefix(kb, ledgerName), lower, upper, 8)
	if err != nil {
		return nil, err
	}

	return &LedgerLogRangeIterator{BoundedEntityIterator: inner}, nil
}

// LedgerLogIterator iterates over log IDs from the RocksDB read index.
// Keys: [0x09][ledger 64B][logID_BE(8B)].
type LedgerLogIterator struct {
	inner *PrefixIterator
}

// NewLedgerLogIterator creates a forward iterator over logs in a ledger.
func NewLedgerLogIterator(reader dal.KVReader, kb *dal.KeyBuilder, ledgerName string) (*LedgerLogIterator, error) {
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

// TxRangeIterator streams canonical transaction IDs in a half-open range.
// Transaction attributes have one key per ID, so plain Next advances without
// deduplication or incrementing the ID (which would wrap at MaxUint64).
type TxRangeIterator struct {
	*BoundedEntityIterator
}

// NewReverseLedgerLogIterator is the descending twin of NewLedgerLogIterator:
// log ids under the ledger's log prefix, high to low. Logs are entity-ordered
// in the index, so the scan streams in both directions (EN-1966).
func NewReverseLedgerLogIterator(reader dal.KVReader, kb *dal.KeyBuilder, ledgerName string) (*ReversePrefixIterator, error) {
	prefix := LedgerLogPrefix(kb, ledgerName)

	return NewReversePrefixIterator(reader, prefix, len(prefix), 8)
}

// NewTxRangeIterator creates a bounded transaction iterator for range queries.
// Nil bounds are open; lower is inclusive and upper is exclusive.
func NewTxRangeIterator(reader dal.KVReader, ledgerName string, lower, upper []byte) (*TxRangeIterator, error) {
	inner, err := NewBoundedEntityIterator(reader, txAttributeCode(ledgerName), lower, upper, 8)
	if err != nil {
		return nil, err
	}

	return &TxRangeIterator{BoundedEntityIterator: inner}, nil
}

// --- transaction prefix helper ---

// txAttributeCode builds the key prefix for scanning transactions
// in a ledger within the attributes zone.
// Format: [ZoneAttributes][SubAttrTransaction][ledgerName padded 64B][separator].
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
func (it *AccountIterator) Direction() (d Asc) { return }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *TxIterator) Direction() (d Asc) { return }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *TxRangeIterator) Direction() (d Asc) { return }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *LedgerLogIterator) Direction() (d Asc) { return }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *ReverseAccountIterator) Direction() (d Desc) { return }

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *ReverseTxIterator) Direction() (d Desc) { return }
