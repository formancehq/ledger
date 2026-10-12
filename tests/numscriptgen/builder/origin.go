package builder

// varOriginDecl is one vars-block declaration whose value comes from the
// compiler (balance()/meta()), not from a runtime-supplied binding. Per the
// numscript grammar, balance()/meta() calls are only legal in this position
// (a var's default) — see NumScript.g4's `origin` rule / Numscript.g4's
// `varOrigin`.
type varOriginDecl struct {
	typ    string
	origin render
}

// declareOriginVar lazily registers a `<typ> $name = <origin>` declaration
// the first time the returned expression is rendered, and reuses the same
// name on subsequent renders (mirroring ExprVar's dedup-by-identity, but
// here identity is just "this closure", since each call site gets its own).
func declareOriginVar[T ExprType](typ string, origin render) Expression[T] {
	allocated := false
	var name string

	return func(env *env, w int) {
		if !allocated {
			id := len(env.originVars)
			name = itemIdToName(id, "originvar")
			env.originVars = append(env.originVars, varOriginDecl{typ: typ, origin: origin})
			allocated = true
		}
		env.builder.WriteByte('$')
		env.builder.WriteString(name)
	}
}

// NewMonetaryVarFromBalance declares a vars-block entry
// `monetary $name = balance(<account>, <asset>)` and returns an expression
// referencing it. balance() reads the account's balance as resolved before
// any statement in the script executes (both engines resolve vars-block
// origins up front, not lazily as the script runs).
func NewMonetaryVarFromBalance(
	account Expression[ExprTypeAccount],
	asset Expression[ExprTypeAsset],
) Expression[ExprTypeMonetary] {
	origin := func(env *env, w int) {
		env.builder.WriteString("balance(")
		account(env, w)
		env.builder.WriteString(", ")
		asset(env, w)
		env.builder.WriteString(")")
	}
	return declareOriginVar[ExprTypeMonetary]("monetary", origin)
}

// varFromMeta declares a vars-block entry `<typ> $name = meta(<account>,
// "<key>")`. meta() is typed by its declaration, not by the stored value, so
// the caller must pass a typ the value actually parses as.
func varFromMeta[T ExprType](typ string, account Expression[ExprTypeAccount], key string) Expression[T] {
	origin := func(env *env, w int) {
		env.builder.WriteString("meta(")
		account(env, w)
		env.builder.WriteString(", ")
		writeStringLiteral(env, key)
		env.builder.WriteString(")")
	}
	return declareOriginVar[T](typ, origin)
}

func NewNumberVarFromMeta(account Expression[ExprTypeAccount], key string) Expression[ExprTypeNumber] {
	return varFromMeta[ExprTypeNumber]("number", account, key)
}

func NewStringVarFromMeta(account Expression[ExprTypeAccount], key string) Expression[ExprTypeString] {
	return varFromMeta[ExprTypeString]("string", account, key)
}

func NewMonetaryVarFromMeta(account Expression[ExprTypeAccount], key string) Expression[ExprTypeMonetary] {
	return varFromMeta[ExprTypeMonetary]("monetary", account, key)
}

func NewAssetVarFromMeta(account Expression[ExprTypeAccount], key string) Expression[ExprTypeAsset] {
	return varFromMeta[ExprTypeAsset]("asset", account, key)
}

func NewAccountVarFromMeta(account Expression[ExprTypeAccount], key string) Expression[ExprTypeAccount] {
	return varFromMeta[ExprTypeAccount]("account", account, key)
}

func NewPortionVarFromMeta(account Expression[ExprTypeAccount], key string) Expression[ExprTypePortion] {
	return varFromMeta[ExprTypePortion]("portion", account, key)
}
