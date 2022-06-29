package core

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"regexp"
)

type Posting struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      int64  `json:"amount"`
	Asset       string `json:"asset"`
}

func (p Posting) Validate() error {
	if p.Amount < 0 {
		return errors.New("negative amount")
	}
	if !IsAddressValid(p.Source) {
		return errors.New("invalid source address")
	}
	if !IsAddressValid(p.Destination) {
		return errors.New("invalid destination address")
	}
	if !AssetIsValid(p.Asset) {
		return errors.New("invalid asset")
	}

	return nil
}

type Postings []Posting

func (ps Postings) Reverse() {
	if len(ps) == 1 {
		ps[0].Source, ps[0].Destination = ps[0].Destination, ps[0].Source
		return
	}
	for i := len(ps)/2 - 1; i >= 0; i-- {
		opp := len(ps) - 1 - i
		ps[i], ps[opp] = ps[opp], ps[i]
		ps[i].Source, ps[i].Destination = ps[i].Destination, ps[i].Source
		ps[opp].Source, ps[opp].Destination = ps[opp].Destination, ps[opp].Source
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

func IsAddressValid(addr string) bool {
	return addressRegexp.Match([]byte(addr))
}
