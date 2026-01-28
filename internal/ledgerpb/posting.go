package ledgerpb

import (
	"errors"
	"math/big"

	"github.com/formancehq/ledger/pkg/accounts"
	"github.com/formancehq/ledger/pkg/assets"
)

// Postings is a slice of Posting pointers
type Postings []*Posting

// Reverse reverses the order of postings and swaps source/destination
func (p Postings) Reverse() Postings {
	postings := make(Postings, len(p))
	copy(postings, p)

	for i := range p {
		if postings[i] != nil {
			postings[i] = &Posting{
				Source:      p[i].Destination,
				Destination: p[i].Source,
				Amount:      p[i].Amount,
				Asset:       p[i].Asset,
			}
		}
	}

	// Reverse the order
	for i := 0; i < len(p)/2; i++ {
		postings[i], postings[len(postings)-i-1] = postings[len(postings)-i-1], postings[i]
	}

	return postings
}

// Validate validates all postings in the slice
func (p Postings) Validate() (int, error) {
	for i, posting := range p {
		if posting == nil {
			return i, errors.New("nil posting")
		}
		if posting.Amount.Value().Cmp(big.NewInt(0)) <= 0 {
			return i, errors.New("no amount defined")
		}
		if !accounts.ValidateAddress(posting.Source) {
			return i, errors.New("invalid source address")
		}
		if !accounts.ValidateAddress(posting.Destination) {
			return i, errors.New("invalid destination address")
		}
		if !assets.IsValid(posting.Asset) {
			return i, errors.New("invalid asset")
		}
	}

	return 0, nil
}

// NewPosting creates a new Posting from the given parameters
func NewPosting(source, destination, asset string, amount *big.Int) *Posting {
	return &Posting{
		Source:      source,
		Destination: destination,
		Amount:      NewBigInt(amount),
		Asset:       asset,
	}
}
