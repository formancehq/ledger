package incarnationmigration

import "github.com/cespare/xxhash/v2"

// accountSeen is a growable, migration-local Bloom filter for incarnation
// keys already present in Pebble. A false positive only causes an extra Get;
// a negative lets the historical fold avoid a costly Get for a new account.
// Layers grow geometrically, so no account count must be known in advance.
type accountSeen struct {
	layers []accountSeenLayer
}

type accountSeenLayer struct {
	bits     []uint64
	capacity uint64
	count    uint64
}

const initialSeenCapacity = 1 << 20
const seenBitsPerKey = 16
const seenHashes = 8

func newAccountSeen() *accountSeen {
	return newAccountSeenWithCapacity(initialSeenCapacity)
}

func newAccountSeenWithCapacity(capacity uint64) *accountSeen {
	if capacity == 0 {
		capacity = 1
	}
	return &accountSeen{layers: []accountSeenLayer{newAccountSeenLayer(capacity)}}
}

func newAccountSeenLayer(capacity uint64) accountSeenLayer {
	// The bit count is a power of two, which keeps each hash probe cheap.
	bitCount := uint64(64)
	for bitCount < capacity*seenBitsPerKey {
		bitCount *= 2
	}
	return accountSeenLayer{bits: make([]uint64, bitCount/64), capacity: capacity}
}

func (s *accountSeen) add(key []byte) {
	layer := &s.layers[len(s.layers)-1]
	if layer.count == layer.capacity {
		s.layers = append(s.layers, newAccountSeenLayer(layer.capacity*2))
		layer = &s.layers[len(s.layers)-1]
	}
	hash := xxhash.Sum64(key)
	for i := uint64(0); i < seenHashes; i++ {
		bit := seenBit(hash, i, uint64(len(layer.bits))*64-1)
		layer.bits[bit/64] |= 1 << (bit % 64)
	}
	layer.count++
}

func (s *accountSeen) mayContain(key []byte) bool {
	hash := xxhash.Sum64(key)
	for i := range s.layers {
		layer := &s.layers[i]
		found := true
		for probe := uint64(0); probe < seenHashes; probe++ {
			bit := seenBit(hash, probe, uint64(len(layer.bits))*64-1)
			if layer.bits[bit/64]&(1<<(bit%64)) == 0 {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}

func seenBit(hash, probe, mask uint64) uint64 {
	step := (hash >> 32) | 1
	return (hash + probe*step) & mask
}
