package builder

// StmtSave: `save <monetary> from <account>`.
func StmtSave(
	monetary Expression[ExprTypeMonetary],
	account Expression[ExprTypeAccount],
) Statement {
	return func(env *env, w int) {
		env.builder.WriteString("save ")
		monetary(env, 0)
		env.builder.WriteString(" from ")
		account(env, w)
	}
}

// StmtSaveAll: `save [<asset> *] from <account>`.
func StmtSaveAll(
	asset Expression[ExprTypeAsset],
	account Expression[ExprTypeAccount],
) Statement {
	return func(env *env, w int) {
		env.builder.WriteString("save [")
		asset(env, 0)
		env.builder.WriteString(" *] from ")
		account(env, w)
	}
}

// StmtSetTxMeta: `set_tx_meta("<key>", <value>)`.
func StmtSetTxMeta[T ExprType](key string, value Expression[T]) Statement {
	return func(env *env, w int) {
		env.builder.WriteString("set_tx_meta(")
		writeStringLiteral(env, key)
		env.builder.WriteString(", ")
		value(env, w)
		env.builder.WriteString(")")
	}
}

// StmtSetAccountMeta: `set_account_meta(<account>, "<key>", <value>)`.
func StmtSetAccountMeta[T ExprType](
	account Expression[ExprTypeAccount],
	key string,
	value Expression[T],
) Statement {
	return func(env *env, w int) {
		env.builder.WriteString("set_account_meta(")
		account(env, w)
		env.builder.WriteString(", ")
		writeStringLiteral(env, key)
		env.builder.WriteString(", ")
		value(env, w)
		env.builder.WriteString(")")
	}
}
