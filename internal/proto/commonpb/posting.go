package commonpb

import (
	"errors"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/holiman/uint256"
)

// Postings is a slice of Posting pointers.
type Postings []*Posting

// Reverse reverses the order of postings and swaps source/destination.
func (p Postings) Reverse() Postings {
	postings := make(Postings, len(p))
	copy(postings, p)

	for i := range p {
		if postings[i] != nil {
			postings[i] = &Posting{
				Source:      p[i].GetDestination(),
				Destination: p[i].GetSource(),
				Amount:      p[i].GetAmount(),
				Asset:       p[i].GetAsset(),
			}
		}
	}

	// Reverse the order
	for i := range len(p) / 2 {
		postings[i], postings[len(postings)-i-1] = postings[len(postings)-i-1], postings[i]
	}

	return postings
}

// Validate validates all postings in the slice.
func (p Postings) Validate() (int, error) {
	for i, posting := range p {
		if posting == nil {
			return i, errors.New("nil posting")
		}

		if posting.GetAmount().IsZero() {
			return i, errors.New("no amount defined")
		}

		if !domain.ValidateAccountAddress(posting.GetSource()) {
			return i, errors.New("invalid source address")
		}

		if !domain.ValidateAccountAddress(posting.GetDestination()) {
			return i, errors.New("invalid destination address")
		}

		if !domain.ValidateAsset(posting.GetAsset()) {
			return i, errors.New("invalid asset")
		}
	}

	return 0, nil
}

// NewPosting creates a new Posting from the given parameters.
// Converts the *big.Int amount to *Uint256 via uint256.Int intermediary.
func NewPosting(source, destination, asset string, amount *big.Int) *Posting {
	var u uint256.Int
	if overflow := u.SetFromBig(amount); overflow {
		panic("commonpb.NewPosting: amount exceeds 256 bits")
	}

	return &Posting{
		Source:      source,
		Destination: destination,
		Amount:      NewUint256(&u),
		Asset:       asset,
	}
}
