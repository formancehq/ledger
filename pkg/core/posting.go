package core

import (
	"database/sql/driver"
	"encoding/json"
	"math/big"
	"regexp"

	"github.com/pkg/errors"
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

func (p Postings) Reverse() {
	for i := range p {
		p[i].Source, p[i].Destination = p[i].Destination, p[i].Source
	}

	for i := 0; i < len(p)/2; i++ {
		p[i], p[len(p)-i-1] = p[len(p)-i-1], p[i]
	}
}

// Scan - Implement the database/sql scanner interface
func (p *Postings) Scan(value interface{}) error {
	if value == nil {
		return nil
	}
	v, err := driver.String.ConvertValue(value)
	if err != nil {
		return err
	}

	*p = Postings{}
	switch vv := v.(type) {
	case []uint8:
		return json.Unmarshal(vv, p)
	case string:
		return json.Unmarshal([]byte(vv), p)
	default:
		panic("not supported type")
	}
}

// Account addresses are composed of segments separated by colons.
// Each segment contains only the following characters: a to z (lower or upper case) and/or digits and/or the special character "_".
var addressRegexp = regexp.MustCompile(`^\w+(:\w+)*$`)

func ValidateAddress(addr string) bool {
	return addressRegexp.Match([]byte(addr))
}

func (p Postings) Validate() (int, error) {
	for i, p := range p {
		if p.Amount.Cmp(Zero) < 0 {
			return i, errors.New("negative amount")
		}
		if !ValidateAddress(p.Source) {
			return i, errors.New("invalid source address")
		}
		if !ValidateAddress(p.Destination) {
			return i, errors.New("invalid destination address")
		}
		if !AssetIsValid(p.Asset) {
			return i, errors.New("invalid asset")
		}
	}

	return 0, nil
}
