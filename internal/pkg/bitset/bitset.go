package bitset

import "encoding/binary"

// Bitset is a compact bitset backed by a []uint64 where each word holds 64 bits.
type Bitset struct {
	words []uint64
}

// New creates a bitset pre-sized to hold at least maxBit+1 bits.
func New(maxBit uint64) *Bitset {
	return &Bitset{
		words: make([]uint64, maxBit/64+1),
	}
}

// Set sets bit at idx and returns the word index that was modified.
func (b *Bitset) Set(idx uint64) uint64 {
	b.grow(idx)
	wi := idx / 64
	b.words[wi] |= 1 << (idx % 64)

	return wi
}

// Test returns true if the bit at idx is set.
func (b *Bitset) Test(idx uint64) bool {
	wi := idx / 64
	if wi >= uint64(len(b.words)) {
		return false
	}

	return b.words[wi]&(1<<(idx%64)) != 0
}

// Word returns the value of the word at the given index.
func (b *Bitset) Word(index uint64) uint64 {
	if index >= uint64(len(b.words)) {
		return 0
	}

	return b.words[index]
}

// SetWord sets the word at the given index. Grows if necessary.
func (b *Bitset) SetWord(index, value uint64) {
	if index >= uint64(len(b.words)) {
		b.grow(index * 64)
	}

	b.words[index] = value
}

// WordCount returns the number of words in the bitset.
func (b *Bitset) WordCount() uint64 {
	return uint64(len(b.words))
}

// Words returns the underlying word slice for direct iteration.
func (b *Bitset) Words() []uint64 {
	return b.words
}

// Clear zeroes all bits.
func (b *Bitset) Clear() {
	clear(b.words)
}

// grow ensures the bitset can hold at least bit idx.
func (b *Bitset) grow(idx uint64) {
	needed := idx/64 + 1
	if uint64(len(b.words)) >= needed {
		return
	}

	if uint64(cap(b.words)) >= needed {
		b.words = b.words[:needed]

		return
	}

	newCap := needed * 2
	newWords := make([]uint64, needed, newCap)
	copy(newWords, b.words)
	b.words = newWords
}

// MarshalWord serializes a single uint64 word as 8 little-endian bytes.
func MarshalWord(w uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, w)

	return buf
}
