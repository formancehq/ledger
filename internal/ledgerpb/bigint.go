package ledgerpb

import "math/big"

func (b *BigInt) UnmarshalJSON(data []byte) error {
	v := big.NewInt(0)
	v.SetString(string(data), 10)
	b.Data = v.Bytes()
	return nil
}

func (b *BigInt) MarshalJSON() ([]byte, error) {
	return []byte(b.Value().String()), nil
}

func (b *BigInt) Value() *big.Int {
	return big.NewInt(0).SetBytes(b.Data)
}

func NewBigInt(v *big.Int) *BigInt {
	return &BigInt{Data: v.Bytes()}
}