package builder

type Statement render

// A bounded send statement
func StmtSend(
	monetary Expression[ExprTypeMonetary],
	source Source,
	destination Destination,
) Statement {
	return func(env *env, w int) {
		env.builder.WriteString("send ")
		monetary(env, 0)
		env.builder.WriteString(" (")
		env.builder.WriteString("\n  source = ")
		source(env, w+1)
		env.builder.WriteString("\n  destination = ")
		destination(env, w+1)
		env.builder.WriteString("\n)")
	}
}

// An unbounded send statement
func StmtSendAll(
	asset Expression[ExprTypeAsset],
	source Source,
	destination Destination,
) Statement {
	return func(env *env, w int) {
		env.builder.WriteString("send [")
		asset(env, 0)
		env.builder.WriteString(" *]")
		env.builder.WriteString(" (")
		env.builder.WriteString("\n  source = ")
		source(env, w+1)
		env.builder.WriteString("\n  destination = ")
		destination(env, w+1)
		env.builder.WriteString("\n)")
	}
}
