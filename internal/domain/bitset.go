package domain

import (
	"encoding/binary"
)

// ReversionBitset is a compact bitset tracking which transaction IDs have been
// reverted within a single ledger. Bit N being set means transaction N is
// reverted. Backed by a []uint64 where each word holds 64 bits.
//
// This is far more memory-efficient than a map: 1 bit per transaction vs ~82
// bytes per entry in a ShardedMap. For 1M transactions, a bitset uses 125 KB.
type ReversionBitset struct {
	words []uint64
}

// NewReversionBitset creates a bitset pre-sized to hold at least maxTxID+1 bits.
func NewReversionBitset(maxTxID uint64) *ReversionBitset {
	return &ReversionBitset{
		words: make([]uint64, wordIndex(maxTxID)+1),
	}
}

// IsReverted returns true if the given transaction ID has been marked as reverted.
func (b *ReversionBitset) IsReverted(txID uint64) bool {
	w := wordIndex(txID)
	if w >= uint64(len(b.words)) {
		return false
	}

	return b.words[w]&(1<<bitIndex(txID)) != 0
}

// SetReverted marks the given transaction ID as reverted and returns the
// word index that was modified so the caller can persist just that word.
func (b *ReversionBitset) SetReverted(txID uint64) uint64 {
	b.grow(txID)
	wi := wordIndex(txID)
	b.words[wi] |= 1 << bitIndex(txID)

	return wi
}

// Word returns the value of the word at the given index.
func (b *ReversionBitset) Word(index uint64) uint64 {
	if index >= uint64(len(b.words)) {
		return 0
	}

	return b.words[index]
}

// WordCount returns the number of words in the bitset.
func (b *ReversionBitset) WordCount() int {
	return len(b.words)
}

// SetWord sets the word at the given index. Grows if necessary.
func (b *ReversionBitset) SetWord(index uint64, value uint64) {
	if index >= uint64(len(b.words)) {
		b.grow(index * 64)
	}

	b.words[index] = value
}

// grow ensures the bitset can hold at least txID.
func (b *ReversionBitset) grow(txID uint64) {
	needed := wordIndex(txID) + 1
	if uint64(len(b.words)) >= needed {
		return
	}

	if uint64(cap(b.words)) >= needed {
		b.words = b.words[:needed]

		return
	}
	// Grow with some headroom to avoid frequent re-allocations.
	newCap := needed * 2
	newWords := make([]uint64, needed, newCap)
	copy(newWords, b.words)
	b.words = newWords
}

func wordIndex(txID uint64) uint64 { return txID / 64 }
func bitIndex(txID uint64) uint64  { return txID % 64 }

// MarshalWord serializes a single uint64 word as 8 little-endian bytes.
func MarshalWord(w uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, w)

	return buf
}
