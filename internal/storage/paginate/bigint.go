package paginate

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/big"
)

type BigInt big.Int

func (i *BigInt) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.ToMathBig())
}

func (i *BigInt) UnmarshalJSON(bytes []byte) error {
	v, err := i.FromString(string(bytes))
	if err != nil {
		return err
	}
	*i = *v
	return nil
}

func NewInt() *BigInt {
	return new(BigInt)
}
func newBigint(x *big.Int) *BigInt {
	return (*BigInt)(x)
}

// same as NewBigint()
func FromMathBig(x *big.Int) *BigInt {
	return (*BigInt)(x)
}

func FromInt64(x int64) *BigInt {
	return FromMathBig(big.NewInt(x))
}

func (i *BigInt) FromString(x string) (*BigInt, error) {
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

func (b *BigInt) Value() (driver.Value, error) {
	return (*big.Int)(b).String(), nil
}

func (b *BigInt) Set(v *BigInt) *BigInt {
	return (*BigInt)((*big.Int)(b).Set((*big.Int)(v)))
}

func (b *BigInt) Sub(x *BigInt, y *BigInt) *BigInt {
	return (*BigInt)((*big.Int)(b).Sub((*big.Int)(x), (*big.Int)(y)))
}

func (b *BigInt) Scan(value interface{}) error {

	var i sql.NullString

	if err := i.Scan(value); err != nil {
		return err
	}

	if _, ok := (*big.Int)(b).SetString(i.String, 10); ok {
		return nil
	}

	return fmt.Errorf("Error converting type %T into Bigint", value)
}

func (b *BigInt) ToMathBig() *big.Int {
	return (*big.Int)(b)
}

func (i *BigInt) Cmp(bottom *BigInt) int {
	return (*big.Int)(i).Cmp((*big.Int)(bottom))
}

var _ json.Unmarshaler = (*BigInt)(nil)
var _ json.Marshaler = (*BigInt)(nil)
