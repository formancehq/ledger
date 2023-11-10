package machine

import (
	"errors"
	"fmt"
	"math/big"
)

type Allotment []big.Rat

func (Allotment) GetType() Type { return TypeAllotment }

func NewAllotment(portions []Portion) (*Allotment, error) {
	n := len(portions)
	total := big.NewRat(0, 1)
	var remainingIdx *int
	allotment := make([]big.Rat, n)
	for i := 0; i < n; i++ {
		if portions[i].Remaining {
			if remainingIdx != nil {
				return nil, errors.New("two uses of `remaining` in the same allotment")
			}
			allotment[i] = big.Rat{} // temporary
			idx := i
			remainingIdx = &idx
		} else {
			rat := *portions[i].Specific
			allotment[i] = rat
			total.Add(total, &rat)
		}
	}
	if total.Cmp(big.NewRat(1, 1)) == 1 {
		return nil, errors.New("sum of portions exceeded 100%")
	}
	if remainingIdx != nil {
		remaining := big.NewRat(1, 1)
		remaining.Sub(remaining, total)
		allotment[*remainingIdx] = *remaining
	}
	result := Allotment(allotment)
	return &result, nil
}

func (a Allotment) String() string {
	out := "{ "
	for i, ratio := range a {
		out += fmt.Sprintf("%v", &ratio)
		if i != len(a)-1 {
			out += " : "
		}
	}
	return out + " }"
}

func (a Allotment) Allocate(amount *MonetaryInt) []*MonetaryInt {
	amtBigint := big.Int(*amount)
	parts := make([]*MonetaryInt, len(a))
	totalAllocated := Zero
	// for every part in the allotment, calculate the floored value
	for i, allot := range a {
		var res big.Int
		res.Mul(&amtBigint, allot.Num())
		res.Div(&res, allot.Denom())
		mi := MonetaryInt(res)
		parts[i] = &mi
		totalAllocated = totalAllocated.Add(parts[i])
	}
	for i := range parts {
		if totalAllocated.Lt(amount) {
			parts[i] = parts[i].Add(NewMonetaryInt(1))
			totalAllocated = totalAllocated.Add(NewMonetaryInt(1))
		}
	}
	return parts
}
