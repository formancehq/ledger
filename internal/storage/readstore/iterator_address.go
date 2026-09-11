package readstore

import (
	"bytes"
	"encoding/binary"
	"slices"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// entitySource is an entity producer whose ORDER IS IRRELEVANT to the
// consumer. addressTxUnion takes one because it builds a set: it drains every
// entity, deduplicates, and sorts the result itself, so an ascending and a
// descending account scan produce the same union. Both Iterator[Asc] and
// Iterator[Desc] satisfy it, which is why the union needs no direction of its
// own (EN-1966).
type entitySource interface {
	Next() bool
	Current() []byte
	Err() error
	Close()
}

// addressTxUnion is the account→transaction union underlying an address match
// on the TRANSACTIONS target. It works by:
//  1. Scanning the existence index for matching account addresses
//  2. For each matching account, scanning the account→tx mapping
//  3. Unioning all transaction ID sets into a single sorted slice
//
// Members come from N per-account scans, each ascending but collectively
// unordered, so "the next transaction after X" is undefined until the whole
// union is known. The union is therefore materialized in full on first use
// and kept for the iterator's lifetime — this is the one materializing leaf
// on the address path, and it is materializing in BOTH directions for the
// same reason.
//
// The slice is built by appending each unseen ID and sorting once at the end,
// so the order during materialization is unspecified; nothing outside
// materialize may observe it. Every positioning call goes through
// ensureMaterialized, which returns only after the slice is sorted.
type addressTxUnion struct {
	reader     dal.PebbleReader
	kb         *dal.KeyBuilder
	ledgerName string
	prefix     byte         // which account→tx prefix to scan
	addrIter   entitySource // produces the matching account addresses
	err        error        // first I/O error from materialize / addrIter

	materialized bool
	txns         [][]byte // all matching txIDs, sorted ascending and deduplicated
}

// ensureMaterialized runs the one-time materialization, latching any I/O error
// (an error is permanent — positioning calls after it always return false).
func (u *addressTxUnion) ensureMaterialized() bool {
	if u.err != nil {
		return false
	}

	if u.materialized {
		return true
	}

	u.materialized = true
	if err := u.materialize(); err != nil {
		u.err = err

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
// underlying Pebble iterators and from the addrIter through addrIter.Err().
func (u *addressTxUnion) materialize() error {
	txSeen := make(map[uint64]struct{})

	for u.addrIter.Next() {
		account := string(u.addrIter.Current())
		prefix := AccountTxPrefix(u.kb, u.prefix, u.ledgerName, account)
		upper := IncrementBytes(prefix)

		iter, err := u.reader.NewIter(&pebble.IterOptions{
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
			u.txns = append(u.txns, txCopy)
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
	slices.SortFunc(u.txns, bytes.Compare)

	return u.addrIter.Err()
}

// AddressTxIterator walks the account→transaction union in D's direction.
//
// Direction costs nothing here: the union is order-insensitive and its result
// is a single ascending sorted slice, so both directions are a
// SliceIterator[D] over that one slice — the descending page needs no second
// collection. Next and Seek are cursor moves over a slice that is stable for
// the iterator's lifetime, so Seek is a true absolute reposition — seekable
// backwards, repeatable, and well-defined after exhaustion — as the Iterator
// contract requires.
type AddressTxIterator[D Direction] struct {
	union *addressTxUnion
	view  *SliceIterator[D]
}

func newAddressTxIterator[D Direction](
	reader dal.PebbleReader,
	kb *dal.KeyBuilder,
	ledgerName string,
	addrIter entitySource,
	prefix byte,
) *AddressTxIterator[D] {
	return &AddressTxIterator[D]{
		union: &addressTxUnion{
			reader:     reader,
			kb:         kb,
			ledgerName: ledgerName,
			prefix:     prefix,
			addrIter:   addrIter,
		},
	}
}

// NewAddressTxIterator creates an iterator that, for each address produced by
// addrIter, looks up all associated transaction IDs in the specified
// account→tx prefix and produces them in ascending order.
func NewAddressTxIterator(
	reader dal.PebbleReader,
	kb *dal.KeyBuilder,
	ledgerName string,
	addrIter entitySource,
	prefix byte,
) *AddressTxIterator[Asc] {
	return newAddressTxIterator[Asc](reader, kb, ledgerName, addrIter, prefix)
}

// NewReverseAddressTxIterator is NewAddressTxIterator in descending order,
// over the same union.
func NewReverseAddressTxIterator(
	reader dal.PebbleReader,
	kb *dal.KeyBuilder,
	ledgerName string,
	addrIter entitySource,
	prefix byte,
) *AddressTxIterator[Desc] {
	return newAddressTxIterator[Desc](reader, kb, ledgerName, addrIter, prefix)
}

// ensureView materializes the union and, on success, borrows its sorted slice.
func (it *AddressTxIterator[D]) ensureView() bool {
	if !it.union.ensureMaterialized() {
		return false
	}

	if it.view == nil {
		it.view = newSliceIterator[D](it.union.txns)
	}

	return true
}

func (it *AddressTxIterator[D]) Next() bool {
	if !it.ensureView() {
		return false
	}

	return it.view.Next()
}

func (it *AddressTxIterator[D]) Current() []byte {
	if it.view == nil {
		return nil
	}

	return it.view.Current()
}

func (it *AddressTxIterator[D]) Seek(target []byte) bool {
	if !it.ensureView() {
		return false
	}

	return it.view.Seek(target)
}

func (it *AddressTxIterator[D]) Err() error {
	if it.union.err != nil {
		return it.union.err
	}

	return it.union.addrIter.Err()
}

func (it *AddressTxIterator[D]) Close() {
	it.union.addrIter.Close()
}

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *AddressTxIterator[D]) Direction() (d D) { return }

var (
	_ EntityIterator  = (*AddressTxIterator[Asc])(nil)
	_ ReverseIterator = (*AddressTxIterator[Desc])(nil)
)
