package readstore

import (
	"encoding/binary"
	"math/bits"

	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
)

// BitsetIterator iterates the set bits of a bitset in ascending order, emitting
// each bit position as an 8-byte big-endian entity ID. It backs the `reverted`
// transaction filter, walking the per-ledger reversion bitset (set bit == a
// reverted transaction ID). Being in-memory, it never errors.
type BitsetIterator struct {
	bs        *bitset.Bitset
	wordIdx   uint64
	remaining uint64 // current word with already-emitted bits cleared
	current   []byte
	started   bool
	done      bool
}

// NewBitsetIterator creates an iterator over the set bits of bs. A nil bitset
// yields no entities.
func NewBitsetIterator(bs *bitset.Bitset) *BitsetIterator {
	return &BitsetIterator{bs: bs}
}

func (it *BitsetIterator) emit() bool {
	tz := uint64(bits.TrailingZeros64(it.remaining))
	it.remaining &= it.remaining - 1 // clear lowest set bit
	it.current = EncodeTxID(nil, it.wordIdx*64+tz)

	return true
}

func (it *BitsetIterator) advance() bool {
	for it.remaining == 0 {
		it.wordIdx++
		if it.wordIdx >= it.bs.WordCount() {
			it.done = true

			return false
		}

		it.remaining = it.bs.Word(it.wordIdx)
	}

	return it.emit()
}

func (it *BitsetIterator) Next() bool {
	if it.done || it.bs == nil {
		return false
	}

	if !it.started {
		it.started = true
		it.wordIdx = 0
		it.remaining = it.bs.Word(0)
	}

	return it.advance()
}

func (it *BitsetIterator) Current() []byte {
	return it.current
}

func (it *BitsetIterator) SeekGE(target []byte) bool {
	if it.bs == nil {
		return false
	}

	// SeekGE is absolute repositioning: recompute the position from target even
	// after a prior walk exhausted the iterator. A latched `done` would make a
	// re-seek to an earlier target (as the NOT/AND merge iterators issue once
	// forward iteration has consumed the bitset) wrongly report no match.
	it.started = true
	it.done = false

	var from uint64
	if len(target) >= 8 {
		from = binary.BigEndian.Uint64(target[:8])
	}

	it.wordIdx = from / 64
	if it.wordIdx >= it.bs.WordCount() {
		it.done = true

		return false
	}

	// Keep only bits at or above the target's position within the start word.
	it.remaining = it.bs.Word(it.wordIdx) &^ ((uint64(1) << (from % 64)) - 1)

	return it.advance()
}

func (it *BitsetIterator) Err() error { return nil }

func (it *BitsetIterator) Close() {}
