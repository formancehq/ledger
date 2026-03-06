package commonpb

import "fmt"

func (t *Target) AsConst() string {
	switch t.GetTarget().(type) {
	case *Target_Account:
		return MetaTargetTypeAccount
	case *Target_Transaction:
		return MetaTargetTypeTransaction
	default:
		panic(fmt.Sprintf("unknown type '%T'", t.GetTarget()))
	}
}
