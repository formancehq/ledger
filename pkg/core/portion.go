package core

import (
	"errors"
	"fmt"
	"math/big"
	"regexp"
)

type Portion struct {
	Remaining bool     `json:"remaining"`
	Specific  *big.Rat `json:"specific"`
}

func (Portion) GetType() Type { return TypePortion }

func NewPortionRemaining() Portion {
	return Portion{
		Remaining: true,
		Specific:  nil,
	}
}

func NewPortionSpecific(r big.Rat) (*Portion, error) {
	if r.Cmp(big.NewRat(0, 1)) != 1 || r.Cmp(big.NewRat(1, 1)) != -1 {
		return nil, errors.New("portion must be between 0% and 100% exclusive")
	}
	return &Portion{
		Remaining: false,
		Specific:  &r,
	}, nil
}

func ValidatePortionSpecific(p Portion) error {
	if p.Remaining {
		return errors.New("remaining should not be true for a specific portion")
	}
	if p.Specific == nil {
		return errors.New("specific portion should not be nil")
	}
	if p.Specific.Cmp(big.NewRat(0, 1)) != 1 || p.Specific.Cmp(big.NewRat(1, 1)) != -1 {
		return errors.New("specific portion must be between 0% and 100% exclusive")
	}
	return nil
}

func (lhs Portion) Equals(rhs Portion) bool {
	if lhs.Remaining != rhs.Remaining {
		return false
	}
	if !lhs.Remaining && lhs.Specific.Cmp(rhs.Specific) != 0 {
		return false
	}
	return true
}

func ParsePortionSpecific(input string) (*Portion, error) {
	var res *big.Rat
	var ok bool

	re := regexp.MustCompile(`^([0-9]+)(?:[.]([0-9]+))?[%]$`)
	percentMatch := re.FindStringSubmatch(input)
	if len(percentMatch) != 0 {
		integral := percentMatch[1]
		fractional := percentMatch[2]
		res, ok = new(big.Rat).SetString(integral + "." + fractional)
		if !ok {
			return nil, errors.New("invalid percent format")
		}
		res.Mul(res, big.NewRat(1, 100))
	} else {
		re = regexp.MustCompile(`^([0-9]+)\s?[/]\s?([0-9]+)$`)
		fractionMatch := re.FindStringSubmatch(input)
		if len(fractionMatch) != 0 {
			numerator := fractionMatch[1]
			denominator := fractionMatch[2]
			res, ok = new(big.Rat).SetString(numerator + "/" + denominator)
			if !ok {
				return nil, errors.New("invalid fractional format")
			}
		}
	}
	if res == nil {
		return nil, errors.New("invalid format")
	}
	return NewPortionSpecific(*res)
}

func (p Portion) String() string {
	if p.Remaining {
		return "remaining"
	}
	return fmt.Sprintf("%v", p.Specific)
}
