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

// SetReverted marks the given transaction ID as reverted.
// Grows the bitset if necessary.
func (b *ReversionBitset) SetReverted(txID uint64) {
	b.grow(txID)
	b.words[wordIndex(txID)] |= 1 << bitIndex(txID)
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

// MarshalWords serializes the words as a packed little-endian byte slice.
func (b *ReversionBitset) MarshalWords() []byte {
	buf := make([]byte, len(b.words)*8)
	for i, w := range b.words {
		binary.LittleEndian.PutUint64(buf[i*8:], w)
	}

	return buf
}

// ReversionBitsetFromWords creates a bitset from a packed little-endian byte slice.
func ReversionBitsetFromWords(data []byte) *ReversionBitset {
	if len(data) == 0 {
		return &ReversionBitset{}
	}

	nWords := len(data) / 8

	words := make([]uint64, nWords)
	for i := range nWords {
		words[i] = binary.LittleEndian.Uint64(data[i*8:])
	}

	return &ReversionBitset{words: words}
}

func wordIndex(txID uint64) uint64 { return txID / 64 }
func bitIndex(txID uint64) uint64  { return txID % 64 }
