package internal

import (
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"strconv"

	antirandom "github.com/antithesishq/antithesis-sdk-go/random"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/internal/protohelpers"
)

// GeometricBulkSize returns a value in [low, high] drawn from a Geometric(p) distribution
// via inverse-CDF sampling: k = ⌊log(u) / log(1−p)⌋ for u ~ Uniform(0,1).
func GeometricBulkSize(p float64, low uint64, high uint64) uint64 {
	u := 1 - Rand().Float64()
	size := min(uint64(math.Floor(math.Log(u)/math.Log(1-p))), high-low)

	return size + low
}

const UserAccountCount uint64 = 32

// Rand returns a *rand.Rand backed by the Antithesis deterministic random source.
// Each call creates a fresh Rand to follow the Antithesis recommendation of
// calling the source at the moment a decision is needed.
func Rand() *rand.Rand {
	return rand.New(antirandom.Source())
}

// RandomBigInt returns a random *big.Int derived from the Antithesis deterministic random source.
func RandomBigInt() *big.Int {
	return new(big.Int).SetUint64(Rand().Uint64())
}

// GetRandomAddress returns a random account address: either "world" or "users:N".
func GetRandomAddress() string {
	return antirandom.RandomChoice([]string{
		"world",
		fmt.Sprintf("users:%d", Rand().Uint64()%UserAccountCount),
	})
}

// RandomPostings generates 1-2 random postings with random sources, destinations, amounts, and assets.
func RandomPostings() []*ledgerpb.Posting {
	r := Rand()

	var postings []*ledgerpb.Posting
	count := r.Uint64()%2 + 1
	for range count {
		source := GetRandomAddress()
		destination := GetRandomAddress()
		amount := RandomBigInt()
		asset := antirandom.RandomChoice([]string{"USD/2", "EUR/2", "COIN"})
		postings = append(postings, protohelpers.NewPosting(source, destination, asset, amount))
	}

	return postings
}

// RandomMetadata generates a random metadata map with 0-2 entries.
func RandomMetadata() map[string]string {
	r := Rand()

	metadata := make(map[string]string)
	for range r.Uint64() % 3 {
		key := strconv.FormatUint(r.Uint64()%999, 10)
		metadata[key] = strconv.FormatUint(r.Uint64()%999, 10)
	}

	return metadata
}
