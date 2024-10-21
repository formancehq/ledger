package ledger

import (
	"github.com/formancehq/ledger/pkg/accounts"
	"github.com/formancehq/ledger/pkg/assets"
	"math/big"

	"errors"
)

type Posting struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Amount      *big.Int `json:"amount"`
	Asset       string   `json:"asset"`
}

func NewPosting(source string, destination string, asset string, amount *big.Int) Posting {
	return Posting{
		Source:      source,
		Destination: destination,
		Amount:      amount,
		Asset:       asset,
	}
}

type Postings []Posting

func (p Postings) Reverse() Postings {
	postings := make(Postings, len(p))
	copy(postings, p)

	for i := range p {
		postings[i].Source, postings[i].Destination = postings[i].Destination, postings[i].Source
	}

	for i := 0; i < len(p)/2; i++ {
		postings[i], postings[len(postings)-i-1] = postings[len(postings)-i-1], postings[i]
	}

	return postings
}

func (p Postings) Validate() (int, error) {
	for i, p := range p {
		if p.Amount == nil {
			return i, errors.New("no amount defined")
		}
		if p.Amount.Cmp(Zero) < 0 {
			return i, errors.New("negative amount")
		}
		if !accounts.ValidateAddress(p.Source) {
			return i, errors.New("invalid source address")
		}
		if !accounts.ValidateAddress(p.Destination) {
			return i, errors.New("invalid destination address")
		}
		if !assets.IsValid(p.Asset) {
			return i, errors.New("invalid asset")
		}
	}

	return 0, nil
}
