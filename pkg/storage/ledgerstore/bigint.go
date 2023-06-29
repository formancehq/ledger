package ledgerstore

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"
)

type Int big.Int

func (i *Int) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.ToMathBig())
}

func (i *Int) UnmarshalJSON(bytes []byte) error {
	v, err := i.FromString(string(bytes))
	if err != nil {
		return err
	}
	*i = *v
	return nil
}

func NewInt() *Int {
	return new(Int)
}
func newBigint(x *big.Int) *Int {
	return (*Int)(x)
}

// same as NewBigint()
func FromMathBig(x *big.Int) *Int {
	return (*Int)(x)
}

func FromInt64(x int64) *Int {
	return FromMathBig(big.NewInt(x))
}

func (i *Int) FromString(x string) (*Int, error) {
	if x == "" {
		return FromInt64(0), nil
	}
	a := big.NewInt(0)
	b, ok := a.SetString(x, 10)

	if !ok {
		return nil, fmt.Errorf("cannot create Int from string")
	}

	return newBigint(b), nil
}

func (b *Int) Value() (driver.Value, error) {
	return (*big.Int)(b).String(), nil
}

func (b *Int) Set(v *Int) *Int {
	return (*Int)((*big.Int)(b).Set((*big.Int)(v)))
}

func (b *Int) Sub(x *Int, y *Int) *Int {
	return (*Int)((*big.Int)(b).Sub((*big.Int)(x), (*big.Int)(y)))
}

func (b *Int) Scan(value interface{}) error {

	var i sql.NullString

	if err := i.Scan(value); err != nil {
		return err
	}

	if _, ok := (*big.Int)(b).SetString(i.String, 10); ok {
		return nil
	}

	return fmt.Errorf("Error converting type %T into Bigint", value)
}

func (b *Int) ToMathBig() *big.Int {
	return (*big.Int)(b)
}

var _ json.Unmarshaler = (*Int)(nil)
var _ json.Marshaler = (*Int)(nil)
