package attributes_test

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
)

// Realistic 5-segment account addresses
var benchInputs = [][]byte{
	[]byte("platform:region-eu:merchant-42:wallets:main"),
	[]byte("banking:customers:usr-8a3f9b2c:savings:EUR"),
	[]byte("marketplace:sellers:shop-12345:escrow:pending"),
	[]byte("corp:divisions:engineering:expenses:travel"),
	[]byte("payments:providers:stripe:settlements:USD"),
	[]byte("treasury:entities:subsidiary-FR:accounts:operating"),
	[]byte("gaming:players:player-xK9mN2:inventory:credits"),
	[]byte("insurance:policies:POL-2024-88291:premiums:collected"),
}

// --- XXH3 (current implementation via KeyHasher) ---

func BenchmarkXXH3_MakeKey(b *testing.B) {
	kh := attributes.NewKeyHasher(attributes.DefaultSeeds)
	for _, input := range benchInputs {
		b.Run(fmt.Sprintf("len=%d", len(input)), func(b *testing.B) {
			for b.Loop() {
				kh.MakeKey(input)
			}
		})
	}
}

// --- BLAKE3 keyed (previous implementation, for reference) ---

type blake3KeyedHasher struct {
	mu        sync.Mutex
	idHasher  *blake3.Hasher
	tagHasher *blake3.Hasher
	idBuf     [32]byte
	tagBuf    [32]byte
}

func newBLAKE3KeyedHasher(masterKey [32]byte) *blake3KeyedHasher {
	idKey := blake3.Sum256(append([]byte("attrid:v1:id128:"), masterKey[:]...))
	tagKey := blake3.Sum256(append([]byte("attrid:v1:tag64:"), masterKey[:]...))
	idHasher, err := blake3.NewKeyed(idKey[:])
	if err != nil {
		panic(err)
	}
	tagHasher, err := blake3.NewKeyed(tagKey[:])
	if err != nil {
		panic(err)
	}
	return &blake3KeyedHasher{
		idHasher:  idHasher,
		tagHasher: tagHasher,
	}
}

func (h *blake3KeyedHasher) makeKey(data []byte) (attributes.U128, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.idHasher.Reset()
	_, _ = h.idHasher.Write(data)
	idSum := h.idHasher.Sum(h.idBuf[:0])

	h.tagHasher.Reset()
	_, _ = h.tagHasher.Write(data)
	tagSum := h.tagHasher.Sum(h.tagBuf[:0])

	lo := binary.LittleEndian.Uint64(idSum[0:8])
	hi := binary.LittleEndian.Uint64(idSum[8:16])
	tag := binary.LittleEndian.Uint64(tagSum[0:8])

	return attributes.NewU128(hi, lo), tag
}

var blake3MasterKey = [32]byte{
	0x3C, 0x3C, 0x69, 0x66, 0x79, 0x6F, 0x75, 0x72,
	0x65, 0x61, 0x64, 0x74, 0x68, 0x69, 0x73, 0x79,
	0x6F, 0x75, 0x66, 0x6F, 0x75, 0x6E, 0x64, 0x61,
	0x73, 0x65, 0x63, 0x72, 0x65, 0x74, 0x3E, 0x3E,
}

func BenchmarkBLAKE3_Keyed_MakeKey(b *testing.B) {
	h := newBLAKE3KeyedHasher(blake3MasterKey)
	for _, input := range benchInputs {
		b.Run(fmt.Sprintf("len=%d", len(input)), func(b *testing.B) {
			for b.Loop() {
				h.makeKey(input)
			}
		})
	}
}

// --- BLAKE3 unkeyed (for comparison) ---

type blake3Unkeyed struct {
	mu     sync.Mutex
	hasher *blake3.Hasher
	buf    [32]byte
}

func (h *blake3Unkeyed) makeKey(data []byte) (attributes.U128, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.hasher.Reset()
	_, _ = h.hasher.Write(data)
	sum := h.hasher.Sum(h.buf[:0])

	lo := binary.LittleEndian.Uint64(sum[0:8])
	hi := binary.LittleEndian.Uint64(sum[8:16])
	tag := binary.LittleEndian.Uint64(sum[16:24])
	return attributes.NewU128(hi, lo), tag
}

func BenchmarkBLAKE3_Unkeyed_MakeKey(b *testing.B) {
	h := &blake3Unkeyed{hasher: blake3.New()}
	for _, input := range benchInputs {
		b.Run(fmt.Sprintf("len=%d", len(input)), func(b *testing.B) {
			for b.Loop() {
				h.makeKey(input)
			}
		})
	}
}

// --- Summary benchmark (one representative input) ---

func BenchmarkHashComparison(b *testing.B) {
	input := []byte("platform:region-eu:merchant-42:wallets:main")

	b.Run("XXH3_Current", func(b *testing.B) {
		kh := attributes.NewKeyHasher(attributes.DefaultSeeds)
		for b.Loop() {
			kh.MakeKey(input)
		}
	})

	b.Run("BLAKE3_Keyed_Reference", func(b *testing.B) {
		h := newBLAKE3KeyedHasher(blake3MasterKey)
		for b.Loop() {
			h.makeKey(input)
		}
	})

	b.Run("BLAKE3_Unkeyed_Reference", func(b *testing.B) {
		h := &blake3Unkeyed{hasher: blake3.New()}
		for b.Loop() {
			h.makeKey(input)
		}
	})
}
