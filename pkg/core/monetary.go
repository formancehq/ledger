package core

import (
	"fmt"
	"math/big"

	"github.com/pkg/errors"
)

type Monetary struct {
	Asset  Asset        `json:"asset"`
	Amount *MonetaryInt `json:"amount"`
}

func (Monetary) GetType() Type { return TypeMonetary }

func (m Monetary) String() string {
	if m.Amount == nil {
		return fmt.Sprintf("[%s nil]", m.Asset)
	}
	amt := *m.Amount
	return fmt.Sprintf("[%v %s]", m.Asset, amt.String())
}

func (m Monetary) GetAsset() Asset { return m.Asset }

func ParseMonetary(mon Monetary) error {
	if err := ParseAsset(mon.Asset); err != nil {
		return errors.Wrapf(err, "asset '%s'", mon.Asset)
	}
	if mon.Amount == nil {
		return errors.Errorf("nil amount")
	}
	if mon.Amount.Ltz() {
		return errors.Errorf("negative amount")
	}
	return nil
}

type MonetaryInt big.Int

func (MonetaryInt) GetType() Type { return TypeNumber }

func (a *MonetaryInt) Add(b *MonetaryInt) *MonetaryInt {
	if a == nil {
		a = NewMonetaryInt(0)
	}

	if b == nil {
		b = NewMonetaryInt(0)
	}

	return (*MonetaryInt)(big.NewInt(0).Add((*big.Int)(a), (*big.Int)(b)))
}

func (a *MonetaryInt) Sub(b *MonetaryInt) *MonetaryInt {
	if a == nil {
		a = NewMonetaryInt(0)
	}

	if b == nil {
		b = NewMonetaryInt(0)
	}

	return (*MonetaryInt)(big.NewInt(0).Sub((*big.Int)(a), (*big.Int)(b)))
}

func (a *MonetaryInt) Neg() *MonetaryInt {
	return (*MonetaryInt)(big.NewInt(0).Neg((*big.Int)(a)))
}

func (a *MonetaryInt) OrZero() *MonetaryInt {
	if a == nil {
		return NewMonetaryInt(0)
	}

	return a
}

func (a *MonetaryInt) Lte(b *MonetaryInt) bool {
	return (*big.Int)(a).Cmp((*big.Int)(b)) <= 0
}

func (a *MonetaryInt) Gte(b *MonetaryInt) bool {
	return (*big.Int)(a).Cmp((*big.Int)(b)) >= 0
}

func (a *MonetaryInt) Lt(b *MonetaryInt) bool {
	return (*big.Int)(a).Cmp((*big.Int)(b)) < 0
}

func (a *MonetaryInt) Ltz() bool {
	return (*big.Int)(a).Cmp(big.NewInt(0)) < 0
}

func (a *MonetaryInt) Gt(b *MonetaryInt) bool {
	return (*big.Int)(a).Cmp((*big.Int)(b)) > 0
}

func (a *MonetaryInt) Eq(b *MonetaryInt) bool {
	return (*big.Int)(a).Cmp((*big.Int)(b)) == 0
}

func (a *MonetaryInt) Equal(b *MonetaryInt) bool {
	return (*big.Int)(a).Cmp((*big.Int)(b)) == 0
}

func (a *MonetaryInt) Cmp(b *MonetaryInt) int {
	return (*big.Int)(a).Cmp((*big.Int)(b))
}

func (a *MonetaryInt) Uint64() uint64 {
	return (*big.Int)(a).Uint64()
}

func (a *MonetaryInt) String() string {
	if a == nil {
		return "0"
	}

	return (*big.Int)(a).String()
}

func (a *MonetaryInt) UnmarshalJSON(b []byte) error {
	return (*big.Int)(a).UnmarshalJSON(b)
}

func (a *MonetaryInt) MarshalJSON() ([]byte, error) {
	if a == nil {
		return []byte("0"), nil
	}
	return (*big.Int)(a).MarshalJSON()
}

func (a *MonetaryInt) MarshalText() ([]byte, error) {
	return (*big.Int)(a).MarshalText()
}

func (a *MonetaryInt) UnmarshalText(b []byte) error {
	return (*big.Int)(a).UnmarshalText(b)
}

func NewMonetaryInt(i int64) *MonetaryInt {
	return (*MonetaryInt)(big.NewInt(i))
}

func ParseMonetaryInt(s string) (*MonetaryInt, error) {
	i, ok := big.NewInt(0).SetString(s, 10)
	if !ok {
		return nil, errors.New("invalid monetary int")
	}

	return (*MonetaryInt)(i), nil
}
