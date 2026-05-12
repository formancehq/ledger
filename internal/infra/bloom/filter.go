// Portions of this file are derived from the blobloom library
// (https://github.com/greatroar/blobloom), copyright 2020-2024 the Blobloom
// authors, licensed under the Apache License, Version 2.0.
//
// Derived elements: block layout ([16]uint32), reducerange, doublehash,
// correctC table, and optimize/FPR estimation functions.
//
// Modifications: custom blocked bloom filter with dirty-block tracking for
// incremental Pebble persistence, atomic word operations for concurrent
// readers, and block-level serialization.

package bloom

import (
	"encoding/binary"
	"math"
	"sync/atomic"
)

// BlockBits is the number of bits per block, chosen to match the L1 cache line
// size of popular architectures (amd64, arm64).
const BlockBits = 512

const (
	wordSize   = 32
	blockWords = BlockBits / wordSize // 16
	blockBytes = blockWords * 4       // 64
)

// block is a fixed-size Bloom filter shard, sized to fit one CPU cache line.
type block [blockWords]uint32

// marshalBlock serializes a block as 16 little-endian uint32 values.
func marshalBlock(b *block) []byte {
	buf := make([]byte, blockBytes)

	for i := range blockWords {
		binary.LittleEndian.PutUint32(buf[4*i:], atomic.LoadUint32(&b[i]))
	}

	return buf
}

// unmarshalBlock deserializes 64 bytes into a block.
func unmarshalBlock(data []byte) block {
	var b block

	for i := range blockWords {
		b[i] = binary.LittleEndian.Uint32(data[4*i:])
	}

	return b
}

// reducerange maps i to an integer in [0, n) using fast modular reduction.
// See https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
func reducerange(i, n uint32) uint32 {
	return uint32((uint64(i) * uint64(n)) >> 32)
}

// doublehash produces the next pair of hash values for enhanced double hashing.
// See https://www.ccs.neu.edu/home/pete/pub/bloom-filters-verification.pdf.
func doublehash(h1, h2 uint32, i int) (uint32, uint32) {
	h1 += h2
	h2 += uint32(i)

	return h1, h2
}

// blockedFilter is a blocked Bloom filter. Concurrent readers (Has) and a
// single writer (Add) are supported lock-free via atomic operations.
type blockedFilter struct {
	blocks []block
	k      int // number of hash probes
}

// newBlockedFilter creates a blocked Bloom filter with the given number of bits
// and hash functions. nbits is rounded up to a multiple of BlockBits.
func newBlockedFilter(nbits uint64, nhashes int) *blockedFilter {
	if nbits < BlockBits {
		nbits = BlockBits
	}

	if nhashes < 2 {
		nhashes = 2
	}

	if nbits%BlockBits != 0 {
		nbits += BlockBits - nbits%BlockBits
	}

	nblocks := nbits / BlockBits

	return &blockedFilter{
		blocks: make([]block, nblocks),
		k:      nhashes,
	}
}

// newBlockedFilterOptimized creates a blocked Bloom filter sized for the given
// capacity and false-positive rate.
func newBlockedFilterOptimized(capacity uint64, fpRate float64) *blockedFilter {
	nbits, nhashes := optimize(capacity, fpRate)

	return newBlockedFilter(nbits, nhashes)
}

// Add inserts a key with hash value h and returns the index of the touched
// block. Safe to call concurrently with Has (lock-free atomics), but only
// from a single writer goroutine.
func (f *blockedFilter) Add(h uint64) uint32 {
	h1, h2 := uint32(h>>32), uint32(h)
	idx := reducerange(h2, uint32(len(f.blocks)))
	b := &f.blocks[idx]

	for i := 1; i < f.k; i++ {
		h1, h2 = doublehash(h1, h2, i)

		bit := uint32(1) << (h1 % wordSize)
		atomic.OrUint32(&b[(h1/wordSize)%blockWords], bit)
	}

	return idx
}

// Has reports whether a key with hash value h might be in the set.
// Thread-safe for concurrent readers (lock-free atomic loads).
func (f *blockedFilter) Has(h uint64) bool {
	h1, h2 := uint32(h>>32), uint32(h)
	idx := reducerange(h2, uint32(len(f.blocks)))
	b := &f.blocks[idx]

	for i := 1; i < f.k; i++ {
		h1, h2 = doublehash(h1, h2, i)

		bit := uint32(1) << (h1 % wordSize)
		if atomic.LoadUint32(&b[(h1/wordSize)%blockWords])&bit == 0 {
			return false
		}
	}

	return true
}

// GetBlock returns a snapshot of the block at the given index (atomic loads).
func (f *blockedFilter) GetBlock(idx uint64) block {
	var snap block

	for j := range blockWords {
		snap[j] = atomic.LoadUint32(&f.blocks[idx][j])
	}

	return snap
}

// SetBlock loads a block from persistent storage at the given index.
func (f *blockedFilter) SetBlock(idx uint64, b block) {
	f.blocks[idx] = b
}

// BlockCount returns the number of blocks.
func (f *blockedFilter) BlockCount() uint64 {
	return uint64(len(f.blocks))
}

// K returns the number of hash probes.
func (f *blockedFilter) K() int {
	return f.k
}

// NumBits returns the total number of bits in the filter.
func (f *blockedFilter) NumBits() uint64 {
	return BlockBits * uint64(len(f.blocks))
}

// --- Optimization (blocked bloom filter sizing) ---

// correctC maps c = m/n for a vanilla Bloom filter to the corrected c' for a
// blocked Bloom filter. From Putze, Sanders and Singler, Table I.
var correctC = []byte{
	1, 1, 2, 4, 5,
	6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 20, 21, 23,
	25, 26, 28, 30, 32, 35, 38, 40, 44, 48, 51, 58, 64, 74, 90,
}

// optimize returns the number of bits and hash functions for a blocked Bloom
// filter with the given capacity and false-positive rate.
func optimize(capacity uint64, fpRate float64) (nbits uint64, nhashes int) {
	if fpRate <= 0 || fpRate > 1 {
		panic("bloom: false positive rate must be > 0 and <= 1")
	}

	n := float64(capacity)
	if n == 0 {
		n = 1
	}

	c := math.Ceil(-math.Log2(fpRate) / math.Ln2)
	if c < float64(len(correctC)) {
		c = float64(correctC[int(c)])
	} else {
		c *= 3
	}

	nbits = uint64(c * n)

	if nbits%BlockBits != 0 {
		nbits += BlockBits - nbits%BlockBits
	}

	if nbits < BlockBits {
		nbits = BlockBits
	}

	// Optimal number of hash functions: k = c * ln(2).
	c = float64(nbits) / n
	k := c * math.Ln2

	if k < 2 {
		return nbits, 2
	}

	// Try rounding up and down; pick the one with lower FPR.
	floorK := math.Floor(k)
	ceilK := math.Ceil(k)

	if ceilK == floorK {
		return nbits, int(ceilK)
	}

	fprFloor := fpRateEstimate(c, floorK)
	fprCeil := fpRateEstimate(c, ceilK)

	if fprFloor < fprCeil {
		return nbits, int(floorK)
	}

	return nbits, int(ceilK)
}

// FPRateEstimate computes the estimated FPR for a blocked Bloom filter.
func FPRateEstimate(nkeys, nbits uint64, nhashes int) float64 {
	if nkeys == 0 {
		return 0
	}

	return fpRateEstimate(float64(nbits)/float64(nkeys), float64(nhashes))
}

// fpRateEstimate implements Putze et al.'s Equation (3).
func fpRateEstimate(c, k float64) float64 {
	const epsilon = 1e-9

	mean := BlockBits / c
	i := math.Ceil(mean)
	p := math.Exp(logPoisson(mean, i) + logFprBlock(BlockBits/i, k))

	for j := i - 1; j > 0; j-- {
		add := math.Exp(logPoisson(mean, j) + logFprBlock(BlockBits/j, k))
		p += add

		if add/p < epsilon {
			break
		}
	}

	for j := i + 1; ; j++ {
		add := math.Exp(logPoisson(mean, j) + logFprBlock(BlockBits/j, k))
		p += add

		if add/p < epsilon {
			break
		}
	}

	return p
}

// logFprBlock returns log((1 - exp(-k/c))^k).
func logFprBlock(c, k float64) float64 {
	return k * math.Log1p(-math.Exp(-k/c))
}

// logPoisson returns log(Poisson(k; lambda)).
func logPoisson(lambda, k float64) float64 {
	lg, _ := math.Lgamma(k + 1)

	return k*math.Log(lambda) - lambda - lg
}
