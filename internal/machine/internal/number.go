package internal

type Number = *MonetaryInt

func NewNumber(i int64) Number {
	return NewMonetaryInt(i)
}

func ParseNumber(s string) (Number, error) {
	return ParseMonetaryInt(s)
}
