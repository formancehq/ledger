package internal

import (
	"fmt"
	"math/big"
	"math/rand"

	antirandom "github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

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
func RandomPostings() []*commonpb.Posting {
	r := Rand()

	var postings []*commonpb.Posting
	count := r.Uint64()%2 + 1
	for range count {
		source := GetRandomAddress()
		destination := GetRandomAddress()
		amount := RandomBigInt()
		asset := antirandom.RandomChoice([]string{"USD/2", "EUR/2", "COIN"})
		postings = append(postings, commonpb.NewPosting(source, destination, asset, amount))
	}
	return postings
}

// RandomMetadata generates a random metadata map with 0-2 entries.
func RandomMetadata() map[string]string {
	r := Rand()

	metadata := make(map[string]string)
	for range r.Uint64() % 3 {
		key := fmt.Sprintf("%v", r.Uint64()%999)
		metadata[key] = fmt.Sprintf("%v", r.Uint64()%999)
	}
	return metadata
}
