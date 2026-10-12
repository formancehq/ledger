package builder

type Destination render

func DestAccount(expr Expression[ExprTypeAccount]) Destination {
	return Destination(expr)
}

// Either `kept` or `to <destination>`, as used in inorder/allotment clauses.
type KeptOrDest render

func Kept() KeptOrDest {
	return func(env *env, w int) {
		env.builder.WriteString("kept")
	}
}

func To(dest Destination) KeptOrDest {
	return func(env *env, w int) {
		env.builder.WriteString("to ")
		dest(env, w)
	}
}

// One `max <amount> <keptOrDest>` clause of a DestInorder block.
type DestInorderClause struct {
	Max  Expression[ExprTypeMonetary]
	Dest KeptOrDest
}

// A destination inorder block. Per the numscript grammar, the `max` clauses
// are mandatory and a trailing `remaining` clause is required.
func DestInorder(clauses []DestInorderClause, remaining KeptOrDest) Destination {
	return func(env *env, w int) {
		env.builder.WriteString("{\n")
		for _, clause := range clauses {
			writeIndentation(env, w+1)
			env.builder.WriteString("max ")
			clause.Max(env, w+1)
			env.builder.WriteString(" ")
			clause.Dest(env, w+1)
			env.builder.WriteByte('\n')
		}
		writeIndentation(env, w+1)
		env.builder.WriteString("remaining ")
		remaining(env, w+1)
		env.builder.WriteByte('\n')
		writeIndentation(env, w)
		env.builder.WriteByte('}')
	}
}

// DestOneof renders `oneof { max ...; remaining ... }` — clause syntax is the
// same as DestInorder's. numscript-only (experimental-oneof feature flag); the
// legacy machine's grammar has no oneof.
func DestOneof(clauses []DestInorderClause, remaining KeptOrDest) Destination {
	return func(env *env, w int) {
		env.builder.WriteString("oneof ")
		DestInorder(clauses, remaining)(env, w)
	}
}

func DestAllotment(clauses ...AllotmentClause[KeptOrDest]) Destination {
	return destAllotment(clauses, nil)
}

// DestAllotmentWithRemaining is DestAllotment with a trailing
// `remaining <keptOrDest>` clause.
//
// The engines constrain the clause differently. The legacy machine's compiler
// rejects it when the known portions already sum to 100%, and rejects a
// portion var in an allotment that lacks it; the interpreter runs the first
// (the clause receives zero) and checks the second's sum at run time. Past
// 100%, both engines reject at run time (oracle/DIVERGENCES.md #6).
// internal/gen keeps generated sums strictly below 100% regardless: the
// machine-side compile rejections above are tolerated skips that compare
// nothing.
func DestAllotmentWithRemaining(clauses []AllotmentClause[KeptOrDest], remaining KeptOrDest) Destination {
	return destAllotment(clauses, remaining)
}

func destAllotment(clauses []AllotmentClause[KeptOrDest], remaining KeptOrDest) Destination {
	return func(env *env, w int) {
		env.builder.WriteString("{\n")
		for _, clause := range clauses {
			writeIndentation(env, w+1)
			clause.Portion(env, w)
			env.builder.WriteString(" ")
			clause.Payload(env, w+1)
			env.builder.WriteByte('\n')
		}
		if remaining != nil {
			writeIndentation(env, w+1)
			env.builder.WriteString("remaining ")
			remaining(env, w+1)
			env.builder.WriteByte('\n')
		}
		writeIndentation(env, w)
		env.builder.WriteByte('}')
	}
}
