package machine

import (
	"errors"
	"fmt"
	"strings"

	collec "github.com/formancehq/go-libs/v3/collectionutils"
)

type FundingPart struct {
	Amount  *MonetaryInt
	Account AccountAddress
}

func (Funding) GetType() Type { return TypeFunding }

func (f Funding) GetAsset() Asset { return f.Asset }

func (lhs FundingPart) Equals(rhs FundingPart) bool {
	return lhs.Account == rhs.Account && lhs.Amount.Equal(rhs.Amount)
}

type Funding struct {
	Asset Asset
	Parts []FundingPart
}

func (lhs Funding) Equals(rhs Funding) bool {
	if lhs.Asset != rhs.Asset {
		return false
	}
	if len(lhs.Parts) != len(rhs.Parts) {
		return false
	}
	for i := range lhs.Parts {
		if !lhs.Parts[i].Equals(rhs.Parts[i]) {
			return false
		}
	}
	return true
}

func (f Funding) String() string {
	out := fmt.Sprintf("[%v", string(f.Asset))
	for _, part := range f.Parts {
		out += fmt.Sprintf(" %v %v", part.Account, part.Amount)
	}
	return out + "]"
}

func (f Funding) Take(amount *MonetaryInt) (Funding, Funding, error) {
	result := Funding{
		Asset: f.Asset,
	}
	remainder := Funding{
		Asset: f.Asset,
	}

	if amount.Eq(Zero) && len(f.Parts) > 0 {
		result.Parts = append(result.Parts, FundingPart{
			Account: f.Parts[0].Account,
			Amount:  amount,
		})
	}

	remainingToWithdraw := amount
	i := 0
	for remainingToWithdraw.Gt(Zero) && i < len(f.Parts) {
		amtToWithdraw := f.Parts[i].Amount
		// if this part has excess balance, put it in the remainder & only take what's needed
		if amtToWithdraw.Gt(remainingToWithdraw) {
			rem := amtToWithdraw.Sub(remainingToWithdraw)
			amtToWithdraw = remainingToWithdraw
			remainder.Parts = append(remainder.Parts, FundingPart{
				Account: f.Parts[i].Account,
				Amount:  rem,
			})
		}
		remainingToWithdraw = remainingToWithdraw.Sub(amtToWithdraw)
		result.Parts = append(result.Parts, FundingPart{
			Account: f.Parts[i].Account,
			Amount:  amtToWithdraw,
		})
		i++
	}
	for i < len(f.Parts) {
		remainder.Parts = append(remainder.Parts, FundingPart{
			Account: f.Parts[i].Account,
			Amount:  f.Parts[i].Amount,
		})
		i++
	}
	if !remainingToWithdraw.Eq(Zero) {

		lstAccounts := collec.Map[FundingPart, string](f.Parts, func(fp FundingPart) string {
			return fp.Account.String()
		})

		return Funding{}, Funding{}, NewErrInsufficientFund("account(s) %s had/have insufficient funds", strings.Join(lstAccounts, "|"))
	}
	return result, remainder, nil
}

func (f Funding) TakeMax(amount *MonetaryInt) (Funding, Funding) {
	result := Funding{
		Asset: f.Asset,
	}
	remainder := Funding{
		Asset: f.Asset,
	}
	remainingToWithdraw := amount
	i := 0
	for remainingToWithdraw.Gt(Zero) && i < len(f.Parts) {
		amtToWithdraw := f.Parts[i].Amount
		// if this part has excess balance, put it in the remainder & only take what's needed
		if amtToWithdraw.Gt(remainingToWithdraw) {
			rem := amtToWithdraw.Sub(remainingToWithdraw)
			amtToWithdraw = remainingToWithdraw
			remainder.Parts = append(remainder.Parts, FundingPart{
				Account: f.Parts[i].Account,
				Amount:  rem,
			})
		}
		remainingToWithdraw = remainingToWithdraw.Sub(amtToWithdraw)
		result.Parts = append(result.Parts, FundingPart{
			Account: f.Parts[i].Account,
			Amount:  amtToWithdraw,
		})
		i++
	}
	for i < len(f.Parts) {
		remainder.Parts = append(remainder.Parts, FundingPart{
			Account: f.Parts[i].Account,
			Amount:  f.Parts[i].Amount,
		})
		i++
	}
	return result, remainder
}

func (f Funding) Concat(other Funding) (Funding, error) {
	if f.Asset != other.Asset {
		return Funding{}, errors.New("tried to concat different assets")
	}
	res := Funding{
		Asset: f.Asset,
		Parts: f.Parts,
	}
	if len(res.Parts) > 0 && len(other.Parts) > 0 && res.Parts[len(res.Parts)-1].Account == other.Parts[0].Account {
		res.Parts[len(res.Parts)-1].Amount = res.Parts[len(res.Parts)-1].Amount.Add(other.Parts[0].Amount)
		res.Parts = append(res.Parts, other.Parts[1:]...)
	} else {
		res.Parts = append(res.Parts, other.Parts...)
	}
	return res, nil
}

func (f Funding) Total() *MonetaryInt {
	total := Zero
	for _, part := range f.Parts {
		total = total.Add(part.Amount)
	}
	return total
}

func (f Funding) Reverse() Funding {
	newParts := []FundingPart{}
	for i := len(f.Parts) - 1; i >= 0; i-- {
		newParts = append(newParts, f.Parts[i])
	}
	newFunding := Funding{
		Asset: f.Asset,
		Parts: newParts,
	}
	return newFunding
}
