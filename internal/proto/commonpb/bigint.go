package commonpb

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/holiman/uint256"
)

func NewBigUintFromUint256(value *uint256.Int) *BigUint {
	if value == nil || value.IsZero() {
		return &BigUint{}
	}

	return &BigUint{Magnitude: value.Bytes()}
}

func NewBigUint(value *big.Int) (*BigUint, error) {
	if value == nil || value.Sign() == 0 {
		return &BigUint{}, nil
	}
	if value.Sign() < 0 {
		return nil, errors.New("big uint cannot be negative")
	}

	return &BigUint{Magnitude: value.Bytes()}, nil
}

// MustBigUintFromDecimal constructs a BigUint from a trusted static decimal.
func MustBigUintFromDecimal(decimal string) *BigUint {
	value, ok := new(big.Int).SetString(decimal, 10)
	if !ok {
		panic(fmt.Sprintf("invalid big uint decimal %q", decimal))
	}
	encoded, err := NewBigUint(value)
	if err != nil {
		panic(err)
	}

	return encoded
}

func (u *BigUint) Validate() error {
	if u == nil {
		return nil
	}
	if len(u.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("big uint contains unknown fields")
	}
	if len(u.GetMagnitude()) > 0 && u.GetMagnitude()[0] == 0 {
		return errors.New("big uint magnitude has a leading zero octet")
	}

	return nil
}

func (u *BigUint) ToBigInt() (*big.Int, error) {
	if err := u.Validate(); err != nil {
		return nil, err
	}
	if u == nil {
		return new(big.Int), nil
	}

	return new(big.Int).SetBytes(u.GetMagnitude()), nil
}

func (u *BigUint) Dec() (string, error) {
	value, err := u.ToBigInt()
	if err != nil {
		return "", err
	}

	return value.String(), nil
}

func (u *BigUint) DecimalString() string {
	decimal, err := u.Dec()
	if err != nil {
		return "<invalid BigUint>"
	}

	return decimal
}

func (u *BigUint) MarshalJSON() ([]byte, error) {
	decimal, err := u.Dec()
	if err != nil {
		return nil, err
	}

	return json.Marshal(decimal)
}

func (u *BigUint) UnmarshalJSON(data []byte) error {
	decimal, err := decodeCanonicalDecimal(data, false)
	if err != nil {
		return err
	}
	value, _ := new(big.Int).SetString(decimal, 10)
	encoded, err := NewBigUint(value)
	if err != nil {
		return err
	}
	u.Reset()
	u.Magnitude = encoded.GetMagnitude()

	return nil
}

func NewSignedBigInt(value *big.Int) *SignedBigInt {
	if value == nil || value.Sign() == 0 {
		return &SignedBigInt{}
	}
	magnitude, _ := NewBigUint(new(big.Int).Abs(value))

	return &SignedBigInt{Negative: value.Sign() < 0, Magnitude: magnitude}
}

// MustSignedBigIntFromDecimal constructs a BigInt from a trusted static decimal.
func MustSignedBigIntFromDecimal(decimal string) *SignedBigInt {
	value, ok := new(big.Int).SetString(decimal, 10)
	if !ok {
		panic(fmt.Sprintf("invalid big int decimal %q", decimal))
	}

	return NewSignedBigInt(value)
}

func (s *SignedBigInt) Validate() error {
	if s == nil {
		return nil
	}
	if err := s.GetMagnitude().Validate(); err != nil {
		return err
	}
	if len(s.ProtoReflect().GetUnknown()) != 0 {
		return errors.New("signed big int contains unknown fields")
	}
	if s.GetMagnitude() != nil && len(s.GetMagnitude().GetMagnitude()) == 0 {
		return errors.New("zero magnitude must be absent from signed big int")
	}
	if s.GetNegative() && s.GetMagnitude() == nil {
		return errors.New("negative zero is not a canonical signed big int")
	}

	return nil
}

func (s *SignedBigInt) ToBigInt() (*big.Int, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}
	if s == nil {
		return new(big.Int), nil
	}
	value, _ := s.GetMagnitude().ToBigInt()
	if s.GetNegative() {
		value.Neg(value)
	}

	return value, nil
}

func (s *SignedBigInt) Dec() (string, error) {
	value, err := s.ToBigInt()
	if err != nil {
		return "", err
	}

	return value.String(), nil
}

func (s *SignedBigInt) DecimalString() string {
	decimal, err := s.Dec()
	if err != nil {
		return "<invalid SignedBigInt>"
	}

	return decimal
}

func (s *SignedBigInt) MarshalJSON() ([]byte, error) {
	decimal, err := s.Dec()
	if err != nil {
		return nil, err
	}

	return json.Marshal(decimal)
}

func (s *SignedBigInt) UnmarshalJSON(data []byte) error {
	decimal, err := decodeCanonicalDecimal(data, true)
	if err != nil {
		return err
	}
	value, _ := new(big.Int).SetString(decimal, 10)
	encoded := NewSignedBigInt(value)
	s.Reset()
	s.Negative = encoded.GetNegative()
	s.Magnitude = encoded.GetMagnitude()

	return nil
}

func decodeCanonicalDecimal(data []byte, signed bool) (string, error) {
	var decimal string
	if err := json.Unmarshal(data, &decimal); err != nil {
		return "", fmt.Errorf("integer must be a quoted decimal string: %w", err)
	}
	if decimal == "" || decimal == "-0" || strings.HasPrefix(decimal, "+") ||
		(strings.HasPrefix(decimal, "0") && len(decimal) > 1) ||
		(strings.HasPrefix(decimal, "-0") && len(decimal) > 2) ||
		(!signed && strings.HasPrefix(decimal, "-")) {
		return "", fmt.Errorf("invalid non-canonical integer %q", decimal)
	}
	digits := strings.TrimPrefix(decimal, "-")
	if digits == "" || strings.IndexFunc(digits, func(r rune) bool { return r < '0' || r > '9' }) >= 0 {
		return "", fmt.Errorf("invalid integer %q", decimal)
	}

	return decimal, nil
}
