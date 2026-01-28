package commonpb

import "fmt"

func (t *Target) AsConst() string {
	switch t.Target.(type) {
	case *Target_Account:
		return MetaTargetTypeAccount
	case *Target_Transaction:
		return MetaTargetTypeTransaction
	default:
		panic(fmt.Sprintf("unknown type '%T'", t.Target))
	}
}
