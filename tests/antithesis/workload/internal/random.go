package internal

import (
	"fmt"
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

const UserAccountCount uint64 = 32

// RandomBigInt returns a random *big.Int derived from the Antithesis deterministic random source.
func RandomBigInt() *big.Int {
	v := random.GetRandom()
	return new(big.Int).SetUint64(v)
}

// GetRandomAddress returns a random account address: either "world" or "users:N".
func GetRandomAddress() string {
	return random.RandomChoice([]string{
		"world",
		fmt.Sprintf("users:%d", random.GetRandom()%UserAccountCount),
	})
}

// RandomPostings generates 1-2 random postings with random sources, destinations, amounts, and assets.
func RandomPostings() []*commonpb.Posting {
	var postings []*commonpb.Posting
	count := random.GetRandom()%2 + 1
	for range count {
		source := GetRandomAddress()
		destination := GetRandomAddress()
		amount := RandomBigInt()
		asset := random.RandomChoice([]string{"USD/2", "EUR/2", "COIN"})
		postings = append(postings, commonpb.NewPosting(source, destination, asset, amount))
	}
	return postings
}

// RandomMetadata generates a random metadata map with 0-2 entries.
func RandomMetadata() map[string]string {
	metadata := make(map[string]string)
	for range random.GetRandom() % 3 {
		key := fmt.Sprintf("%v", random.GetRandom()%999)
		metadata[key] = fmt.Sprintf("%v", random.GetRandom()%999)
	}
	return metadata
}
