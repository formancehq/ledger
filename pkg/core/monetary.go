package core

type MonetaryInt int64

func (a MonetaryInt) Add(b MonetaryInt) MonetaryInt {
	return a + b
}

func (a MonetaryInt) Sub(b MonetaryInt) MonetaryInt {
	return a - b
}

func (a MonetaryInt) Neg() MonetaryInt {
	return -a
}

func (a MonetaryInt) Lte(b MonetaryInt) bool {
	return a <= b
}

func (a MonetaryInt) Gte(b MonetaryInt) bool {
	return a >= b
}

func (a MonetaryInt) Lt(b MonetaryInt) bool {
	return a < b
}

func (a MonetaryInt) Gt(b MonetaryInt) bool {
	return a > b
}

func (a MonetaryInt) Eq(b MonetaryInt) bool {
	return a == b
}

func NewMonetaryInt(i int64) MonetaryInt {
	return MonetaryInt(i)
}
