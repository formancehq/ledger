package ledgerpb

import (
	"errors"
	"fmt"
	"math/big"
)

// ToBigInt decodes the canonical unsigned magnitude used on the wire.
func (x *BigUint) ToBigInt() (*big.Int, error) {
	if x == nil || len(x.GetMagnitude()) == 0 {
		return new(big.Int), nil
	}
	if len(x.Magnitude) > 32 || x.Magnitude[0] == 0 {
		return nil, errors.New("invalid BigUint magnitude")
	}
	return new(big.Int).SetBytes(x.Magnitude), nil
}

func NewBigUint(value *big.Int) *BigUint {
	if value == nil || value.Sign() < 0 {
		panic(fmt.Sprintf("value does not fit in BigUint: %v", value))
	}
	return &BigUint{Magnitude: value.Bytes()}
}

func MustBigUintFromDecimal(decimal string) *BigUint {
	value, ok := new(big.Int).SetString(decimal, 10)
	if !ok || value.Sign() < 0 {
		panic(fmt.Sprintf("invalid BigUint decimal %q", decimal))
	}
	return NewBigUint(value)
}

func NewSignedBigInt(value *big.Int) *SignedBigInt {
	if value == nil {
		value = new(big.Int)
	}
	return &SignedBigInt{Negative: value.Sign() < 0, Magnitude: NewBigUint(new(big.Int).Abs(value))}
}

func (x *SignedBigInt) ToBigInt() (*big.Int, error) {
	if x == nil || x.GetMagnitude() == nil {
		return new(big.Int), nil
	}
	value, err := x.GetMagnitude().ToBigInt()
	if err != nil {
		return nil, err
	}
	if x.GetNegative() {
		value.Neg(value)
	}
	return value, nil
}

func (v *Volumes) Validate() error {
	if v == nil || v.GetInput() == nil || v.GetOutput() == nil {
		return errors.New("volume input and output must be present")
	}
	if _, err := v.GetInput().ToBigInt(); err != nil {
		return fmt.Errorf("invalid input volume: %w", err)
	}
	if _, err := v.GetOutput().ToBigInt(); err != nil {
		return fmt.Errorf("invalid output volume: %w", err)
	}
	return nil
}
