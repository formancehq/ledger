package readstore

import (
	"bytes"
	"encoding/binary"
	"slices"
	"sort"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// AddressTxIterator translates an address match on the TRANSACTIONS target into
// a sorted iterator of transaction IDs. It works by:
//  1. Scanning the existence index for matching account addresses
//  2. For each matching account, scanning the account→tx mapping
//  3. Unioning all transaction ID sets into a single sorted output
//
// The union is built by appending each unseen ID and sorting the completed
// slice once, so the order during materialization is unspecified; nothing
// outside materialize may observe it. Every positioning call goes through
// ensureMaterialized, which returns only after the slice is sorted.
//
// The union is materialized in full on first use and kept for the iterator's
// lifetime; Next and Seek are cursor moves over the stable sorted slice, so
// Seek is a true absolute reposition — seekable backwards, repeatable, and
// well-defined after exhaustion — as the EntityIterator contract requires.
type AddressTxIterator struct {
	reader     dal.PebbleReader
	kb         *dal.KeyBuilder
	ledgerName string
	prefix     byte           // which account→tx prefix to scan
	addrIter   EntityIterator // iterates over matching account addresses
	current    []byte         // current txID (8 bytes)
	err        error          // first I/O error from materialize / addrIter

	materialized bool
	txns         [][]byte // all matching txIDs, sorted and deduplicated
	pos          int      // index into txns of the entry the next Next() yields
}

// NewAddressTxIterator creates an iterator that, for each address matching
// addrIter, looks up all associated transaction IDs in the specified
// account→tx prefix and produces them in sorted order.
func NewAddressTxIterator(
	reader dal.PebbleReader,
	kb *dal.KeyBuilder,
	ledgerName string,
	addrIter EntityIterator,
	prefix byte,
) *AddressTxIterator {
	return &AddressTxIterator{
		reader:     reader,
		kb:         kb,
		ledgerName: ledgerName,
		prefix:     prefix,
		addrIter:   addrIter,
	}
}

func (it *AddressTxIterator) Next() bool {
	if !it.ensureMaterialized() {
		return false
	}

	if it.pos >= len(it.txns) {
		return false
	}

	it.current = it.txns[it.pos]
	it.pos++

	return true
}

func (it *AddressTxIterator) Current() []byte {
	return it.current
}

func (it *AddressTxIterator) Seek(target []byte) bool {
	if !it.ensureMaterialized() {
		return false
	}

	idx := sort.Search(len(it.txns), func(i int) bool {
		return bytes.Compare(it.txns[i], target) >= 0
	})
	if idx >= len(it.txns) {
		it.pos = len(it.txns)

		return false
	}

	it.current = it.txns[idx]
	it.pos = idx + 1

	return true
}

func (it *AddressTxIterator) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.addrIter.Err()
}

func (it *AddressTxIterator) Close() {
	it.addrIter.Close()
}

// ensureMaterialized runs the one-time materialization, latching any I/O error
// (an error is permanent — positioning calls after it always return false).
func (it *AddressTxIterator) ensureMaterialized() bool {
	if it.err != nil {
		return false
	}

	if it.materialized {
		return true
	}

	it.materialized = true
	if err := it.materialize(); err != nil {
		it.err = err

		return false
	}

	return true
}

// materialize collects all transaction IDs from all matching accounts,
// deduplicates them through txSeen, appends each unseen ID as an owned copy
// (never a retained Pebble iterator key buffer), and sorts the completed slice
// once. Sorting on each insertion instead (a binary search plus a tail shift)
// moves O(U^2) elements for U unique IDs when account histories interleave,
// because most new IDs land near the front. Surfaces I/O errors from the
// underlying Pebble iterators and from the addrIter through addrIter.Err()
// (checked by the caller via it.Err()).
func (it *AddressTxIterator) materialize() error {
	txSeen := make(map[uint64]struct{})

	for it.addrIter.Next() {
		account := string(it.addrIter.Current())
		prefix := AccountTxPrefix(it.kb, it.prefix, it.ledgerName, account)
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

			if _, seen := txSeen[txID]; seen {
				continue
			}

			txSeen[txID] = struct{}{}

			txCopy := make([]byte, 8)
			copy(txCopy, txIDBytes)
			it.txns = append(it.txns, txCopy)
		}

		iterErr := iter.Error()
		_ = iter.Close()

		if iterErr != nil {
			return iterErr
		}
	}

	// Sort once, on every non-error path, before any consumer can reach the
	// slice. IDs are unique after txSeen, so no tie can be reordered and an
	// unstable sort is safe. bytes.Compare on the 8-byte big-endian IDs is the
	// numeric ID order (see ReadStoreComparer).
	slices.SortFunc(it.txns, bytes.Compare)

	return it.addrIter.Err()
}
