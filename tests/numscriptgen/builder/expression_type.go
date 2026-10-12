package builder

type ExprType interface {
	exprType()
}

type ExprTypeString interface {
	ExprType
	string()
}
type ExprTypeAccount interface {
	ExprType
	account()
}
type ExprTypeAsset interface {
	ExprType
	asset()
}
type ExprTypeNumber interface {
	ExprType
	number()
}
type ExprTypeMonetary interface {
	ExprType
	monetary()
}
type ExprTypePortion interface {
	ExprType
	portion()
}
