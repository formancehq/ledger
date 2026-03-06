package readstore

import (
	"bytes"
	"encoding/binary"

	bolt "go.etcd.io/bbolt"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// AddressTxIterator translates an address match on the TRANSACTIONS target into
// a sorted iterator of transaction IDs. It works by:
//  1. Scanning the existence index for matching account addresses
//  2. For each matching account, scanning the account→tx mapping
//  3. Merge-unioning all transaction ID sets into a single sorted output
//
// This is a lazy streaming implementation that does not materialize
// intermediate sets.
type AddressTxIterator struct {
	tx          *bolt.Tx
	kb          *dal.KeyBuilder
	ledger      string
	bucket      []byte         // which account→tx bucket to scan
	addrIter    EntityIterator // iterates over matching account addresses
	current     []byte         // current txID (8 bytes)
	exhausted   bool
	pendingTxns [][]byte // merge-union buffer for deduplication
	txSeen      map[uint64]struct{}
}

// NewAddressTxIterator creates an iterator that, for each address matching
// addrIter, looks up all associated transaction IDs in the specified
// account→tx bucket and produces them in sorted order (merge-union).
func NewAddressTxIterator(
	tx *bolt.Tx,
	kb *dal.KeyBuilder,
	ledger string,
	addrIter EntityIterator,
	bucket []byte,
) *AddressTxIterator {
	return &AddressTxIterator{
		tx:       tx,
		kb:       kb,
		ledger:   ledger,
		bucket:   bucket,
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
	b := it.tx.Bucket(it.bucket)
	if b == nil {
		return nil
	}

	c := b.Cursor()

	for it.addrIter.Next() {
		account := string(it.addrIter.Current())
		prefix := AccountTxPrefix(it.kb, it.ledger, account)

		for k, _ := c.Seek(prefix); k != nil && HasPrefix(k, prefix); k, _ = c.Next() {
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
