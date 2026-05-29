package readstore

import (
	"bytes"
	"encoding/binary"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// AddressTxIterator translates an address match on the TRANSACTIONS target into
// a sorted iterator of transaction IDs. It works by:
//  1. Scanning the existence index for matching account addresses
//  2. For each matching account, scanning the account→tx mapping
//  3. Merge-unioning all transaction ID sets into a single sorted output
type AddressTxIterator struct {
	reader      dal.PebbleReader
	kb          *dal.KeyBuilder
	ledgerID    uint32
	prefix      byte           // which account→tx prefix to scan
	addrIter    EntityIterator // iterates over matching account addresses
	current     []byte         // current txID (8 bytes)
	exhausted   bool
	pendingTxns [][]byte // merge-union buffer for deduplication
	txSeen      map[uint64]struct{}
}

// NewAddressTxIterator creates an iterator that, for each address matching
// addrIter, looks up all associated transaction IDs in the specified
// account→tx prefix and produces them in sorted order (merge-union).
func NewAddressTxIterator(
	reader dal.PebbleReader,
	kb *dal.KeyBuilder,
	ledgerID uint32,
	addrIter EntityIterator,
	prefix byte,
) *AddressTxIterator {
	return &AddressTxIterator{
		reader:   reader,
		kb:       kb,
		ledgerID: ledgerID,
		prefix:   prefix,
		addrIter: addrIter,
		txSeen:   make(map[uint64]struct{}),
	}
}

func (it *AddressTxIterator) Next() bool {
	if it.exhausted {
		return false
	}

	// Try to get next tx from pending buffer
	if len(it.pendingTxns) > 0 {
		it.current = it.pendingTxns[0]
		it.pendingTxns = it.pendingTxns[1:]

		return true
	}

	// Materialize all txIDs for remaining accounts and merge-union
	err := it.materialize()
	if err != nil || len(it.pendingTxns) == 0 {
		it.exhausted = true

		return false
	}

	it.current = it.pendingTxns[0]
	it.pendingTxns = it.pendingTxns[1:]

	return true
}

func (it *AddressTxIterator) Current() []byte {
	return it.current
}

func (it *AddressTxIterator) SeekGE(target []byte) bool {
	if it.exhausted {
		return false
	}

	// Re-materialize with filter
	if len(it.pendingTxns) == 0 {
		err := it.materialize()
		if err != nil || len(it.pendingTxns) == 0 {
			it.exhausted = true

			return false
		}
	}

	// Binary search in sorted pendingTxns
	idx := 0
	for idx < len(it.pendingTxns) {
		if bytes.Compare(it.pendingTxns[idx], target) >= 0 {
			break
		}

		idx++
	}

	if idx >= len(it.pendingTxns) {
		it.exhausted = true

		return false
	}

	it.current = it.pendingTxns[idx]
	it.pendingTxns = it.pendingTxns[idx+1:]

	return true
}

func (it *AddressTxIterator) Close() {
	it.addrIter.Close()
}

// materialize collects all transaction IDs from all matching accounts,
// deduplicates, and sorts them.
func (it *AddressTxIterator) materialize() error {
	for it.addrIter.Next() {
		account := string(it.addrIter.Current())
		prefix := AccountTxPrefix(it.kb, it.prefix, it.ledgerID, account)
		upper := IncrementBytes(prefix)

		iter, err := it.reader.NewIter(&pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: upper,
		})
		if err != nil {
			return err
		}

		for iter.First(); iter.Valid(); iter.Next() {
			k := iter.Key()
			// Extract txID from the suffix (last 8 bytes)
			if len(k) < len(prefix)+8 {
				continue
			}

			txIDBytes := k[len(k)-8:]
			txID := binary.BigEndian.Uint64(txIDBytes)

			if _, seen := it.txSeen[txID]; seen {
				continue
			}

			it.txSeen[txID] = struct{}{}

			txCopy := make([]byte, 8)
			copy(txCopy, txIDBytes)
			it.pendingTxns = insertSorted(it.pendingTxns, txCopy)
		}

		_ = iter.Close()
	}

	return nil
}

// insertSorted inserts a value into a sorted slice maintaining sort order.
func insertSorted(slice [][]byte, val []byte) [][]byte {
	// Find insertion point via binary search
	lo, hi := 0, len(slice)
	for lo < hi {
		mid := (lo + hi) / 2
		if bytes.Compare(slice[mid], val) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	// Insert at position lo
	slice = append(slice, nil)
	copy(slice[lo+1:], slice[lo:])
	slice[lo] = val

	return slice
}
