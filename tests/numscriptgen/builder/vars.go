package builder

import "math/big"

type var_[T ExprType] struct {
	alloc func(env *env) string
}

type anyVar interface {
	anyVar()
}

func (*Var[T]) anyVar() {}

type VarsEnv struct {
	bindings map[anyVar]string
}

type Var[T ExprType] var_[ExprType]

func NewAccountVar() Var[ExprTypeAccount] {
	return Var[ExprTypeAccount]{
		alloc: func(env *env) string {
			id := env.accountsPool.getFreshId()
			return accountToName(id)
		},
	}
}

func NewAssetVar() Var[ExprTypeAsset] {
	return Var[ExprTypeAsset]{
		alloc: func(env *env) string {
			id := env.assetsPool.getFreshId()
			return assetToName(id)
		},
	}
}

func NewStringVar() Var[ExprTypeString] {
	return Var[ExprTypeString]{
		alloc: func(env *env) string {
			id := env.stringsPool.getFreshId()
			return stringToName(id)
		},
	}
}

func NewNumberVar() Var[ExprTypeNumber] {
	return Var[ExprTypeNumber]{
		alloc: func(env *env) string {
			id := env.numbersPool.getFreshId()
			return numberToName(id)
		},
	}
}

func (v VarsEnv) FillAccount(var_ *Var[ExprTypeAccount], account string) (string, string) {
	name := v.bindings[anyVar(var_)]
	return name, account
}

func (v VarsEnv) FillAsset(var_ *Var[ExprTypeAsset], asset string) (string, string) {
	name := v.bindings[anyVar(var_)]
	return name, asset
}

func (v VarsEnv) FillString(var_ *Var[ExprTypeString], str string) (string, string) {
	name := v.bindings[anyVar(var_)]
	return name, str
}

func (v VarsEnv) FillNumber(var_ *Var[ExprTypeNumber], bi *big.Int) (string, string) {
	name := v.bindings[anyVar(var_)]
	return name, bi.String()
}
