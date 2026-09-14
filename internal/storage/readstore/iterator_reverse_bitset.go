package readstore

import (
	"encoding/binary"
	"math/bits"

	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
)

// ReverseBitsetIterator iterates the set bits of a bitset in DESCENDING order,
// emitting each bit position as an 8-byte big-endian entity ID — the mirror of
// BitsetIterator. It backs the `reverted` transaction filter on descending
// pages. Being in-memory, it never errors.
type ReverseBitsetIterator struct {
	bs      *bitset.Bitset
	wordIdx uint64
	// remaining is the current word with already-emitted (higher) bits cleared.
	remaining uint64
	current   []byte
	started   bool
	done      bool
}

// NewReverseBitsetIterator creates a descending iterator over the set bits of
// bs. A nil bitset yields no entities.
func NewReverseBitsetIterator(bs *bitset.Bitset) *ReverseBitsetIterator {
	return &ReverseBitsetIterator{bs: bs}
}

// emit yields the HIGHEST remaining set bit of the current word.
func (it *ReverseBitsetIterator) emit() bool {
	pos := uint64(63 - bits.LeadingZeros64(it.remaining))
	it.remaining &^= uint64(1) << pos
	it.current = EncodeTxID(nil, it.wordIdx*64+pos)

	return true
}

// advance walks down to the next word holding a set bit.
func (it *ReverseBitsetIterator) advance() bool {
	for it.remaining == 0 {
		if it.wordIdx == 0 {
			it.done = true

			return false
		}

		it.wordIdx--
		it.remaining = it.bs.Word(it.wordIdx)
	}

	return it.emit()
}

func (it *ReverseBitsetIterator) Next() bool {
	if it.done || it.bs == nil {
		return false
	}

	if !it.started {
		it.started = true

		wc := it.bs.WordCount()
		if wc == 0 {
			it.done = true

			return false
		}

		it.wordIdx = wc - 1
		it.remaining = it.bs.Word(it.wordIdx)
	}

	return it.advance()
}

func (it *ReverseBitsetIterator) Current() []byte { return it.current }

// Seek positions at the highest set bit <= target. Like BitsetIterator.SeekGE
// it is absolute repositioning: the position is recomputed from target even
// after a prior walk exhausted the iterator, because the composite reverse
// merge iterators re-seek children freely.
func (it *ReverseBitsetIterator) Seek(target []byte) bool {
	if it.bs == nil {
		return false
	}

	it.started = true
	it.done = false

	wc := it.bs.WordCount()
	if wc == 0 {
		it.done = true

		return false
	}

	// A target shorter than an encoded entity cannot address a bit above 0;
	// mirroring BitsetIterator.SeekGE, it reads as position 0.
	var from uint64
	if len(target) >= 8 {
		from = binary.BigEndian.Uint64(target[:8])
	}

	it.wordIdx = from / 64
	if it.wordIdx >= wc {
		// Target is past every stored word: start from the top.
		it.wordIdx = wc - 1
		it.remaining = it.bs.Word(it.wordIdx)
	} else {
		// Keep only bits at or below the target's position within its word.
		shift := from % 64
		it.remaining = it.bs.Word(it.wordIdx) & (^uint64(0) >> (63 - shift))
	}

	return it.advance()
}

func (it *ReverseBitsetIterator) Err() error { return nil }

func (it *ReverseBitsetIterator) Close() {}

// Direction is the compile-time direction witness; see Iterator.Direction.
func (it *ReverseBitsetIterator) Direction() (d Desc) { return }

var _ ReverseIterator = (*ReverseBitsetIterator)(nil)
