package core

import (
	"fmt"
	"reflect"
)

type Type byte

const (
	TypeAccount   = Type(iota + 1) // address of an account
	TypeAsset                      // name of an asset
	TypeNumber                     // 64bit unsigned integer
	TypeString                     // string
	TypeMonetary                   // [asset number]
	TypePortion                    // rational number between 0 and 1 both exclusive
	TypeAllotment                  // list of portions
	TypeAmount                     // either ALL or a SPECIFIC number
	TypeFunding                    // (asset, []{amount, account})
)

func (t Type) String() string {
	switch t {
	case TypeAccount:
		return "account"
	case TypeAsset:
		return "asset"
	case TypeNumber:
		return "number"
	case TypeString:
		return "string"
	case TypeMonetary:
		return "monetary"
	case TypePortion:
		return "portion"
	case TypeAllotment:
		return "allotment"
	case TypeAmount:
		return "amount"
	default:
		return "invalid type"
	}
}

type Value interface {
	GetType() Type
}

type String string

func (String) GetType() Type { return TypeString }
func (s String) String() string {
	return fmt.Sprintf("\"%v\"", string(s))
}

func ValueEquals(lhs, rhs Value) bool {
	if reflect.TypeOf(lhs) != reflect.TypeOf(rhs) {
		return false
	}
	if lhsn, ok := lhs.(*MonetaryInt); ok {
		rhsn := rhs.(*MonetaryInt)
		return lhsn.Equal(rhsn)
	} else if lhsm, ok := lhs.(Monetary); ok {
		rhsm := rhs.(Monetary)
		return lhsm.Asset == rhsm.Asset && lhsm.Amount.Equal(rhsm.Amount)
	} else if lhsa, ok := lhs.(Allotment); ok {
		rhsa := rhs.(Allotment)
		if len(lhsa) != len(rhsa) {
			return false
		}
		for i := range lhsa {
			if lhsa[i].Cmp(&rhsa[i]) != 0 {
				return false
			}
		}
	} else if lhsp, ok := lhs.(Portion); ok {
		rhsp := rhs.(Portion)
		return lhsp.Equals(rhsp)
	} else if lhsf, ok := lhs.(Funding); ok {
		rhsf := rhs.(Funding)
		return lhsf.Equals(rhsf)
	} else if lhs != rhs {
		return false
	}
	return true
}
