// Builder to format numscript document
package builder

import (
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

const indentStr = "  "

func (p *pool[T]) getFreshId() int {
	id := p.nextId
	p.nextId += 1
	return id
}

func (p *pool[T]) getItemId(elem T) int {
	previousLookup, isElemInPool := p.elems[elem]
	if !isElemInPool {
		elemId := p.getFreshId()
		p.elems[elem] = elemId
		previousLookup = elemId
	}
	return previousLookup
}

type env struct {
	builder      strings.Builder
	accountsPool pool[string]
	assetsPool   pool[string]
	stringsPool  pool[string]
	numbersPool  pool[*big.Int]
	// Monetaries and portions are pooled by their textual binding ("COIN 100",
	// "1/2") rather than by a structured value: the binding map is strings
	// anyway, and equal text is the same value.
	monetariesPool pool[string]
	portionsPool   pool[string]
	varsEnv        VarsEnv
	originVars     []varOriginDecl
}

func writeIndentation(env *env, w int) {
	if w == 0 {
		return
	}

	env.builder.Grow(w * len(indentStr))
	for range w {
		env.builder.WriteString(indentStr)
	}
}

func newEnv() env {
	return env{
		accountsPool:   newPool[string](),
		assetsPool:     newPool[string](),
		stringsPool:    newPool[string](),
		numbersPool:    newPool[*big.Int](),
		monetariesPool: newPool[string](),
		portionsPool:   newPool[string](),
		varsEnv:        VarsEnv{bindings: map[anyVar]string{}},
	}
}

// The underlying type of any a pretty printing document
type render = func(
	env *env,

	// The current width
	w int,
)

func itemIdToName(id int, prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, id)
}
func accountToName(id int) string {
	return itemIdToName(id, "account")
}
func assetToName(id int) string {
	return itemIdToName(id, "asset")
}
func stringToName(id int) string {
	return itemIdToName(id, "string")
}
func monetaryToName(id int) string {
	return itemIdToName(id, "monetary")
}
func portionToName(id int) string {
	return itemIdToName(id, "portion")
}
func numberToName(id int) string {
	return itemIdToName(id, "number")
}

type varRenderState struct {
	hasVars       bool
	sb            strings.Builder
	knownBindings map[string]string
}

func renderVar[T comparable](
	st *varRenderState,

	typ string,
	pool pool[T],
	getVarName func(id int) string,
	stringifyValue func(value T) string,
) {
	for key, id := range pool.elems {
		varName := getVarName(id)
		st.knownBindings[varName] = stringifyValue(key)
	}

	for id := range pool.nextId {
		st.hasVars = true

		varName := getVarName(id)

		st.sb.WriteString(indentStr)
		st.sb.WriteString(typ)
		st.sb.WriteString(" $")
		st.sb.WriteString(varName)
		st.sb.WriteByte('\n')
	}

}

func stringId(x string) string { return x }

// writeStringLiteral writes s as a raw double-quoted numscript string
// literal directly into env.builder, bypassing the vars pool (mirrors
// UnsafeAccount's rationale: some grammar positions, like a meta key or a
// set_tx_meta/set_account_meta key, only ever take a literal, never a
// variable).
func writeStringLiteral(env *env, s string) {
	env.builder.WriteByte('"')
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '"' || ch == '\\' {
			env.builder.WriteByte('\\')
		}
		env.builder.WriteByte(ch)
	}
	env.builder.WriteByte('"')
}

// renderExprToString renders r against env (which may consult/populate
// env's pools as a side effect, same as rendering it into the program body
// would) and returns the resulting text, without disturbing env.builder's
// actual accumulated content.
func renderExprToString(env *env, r render) string {
	saved := env.builder
	env.builder = strings.Builder{}
	r(env, 0)
	out := env.builder.String()
	env.builder = saved
	return out
}

// originVarRefRe matches a reference to an origin var inside a rendered
// origin RHS, capturing its id.
var originVarRefRe = regexp.MustCompile(`\$originvar_(\d+)\b`)

// originDeclOrder orders origin var ids so that each one is declared after
// every origin var its RHS references. Both engines resolve vars-block
// origins in declaration order and dereference a referenced resource without
// checking that it resolved, so a forward reference is a nil dereference
// rather than a diagnostic.
//
// Ids are allocated dependent-first (the outer origin is rendered before the
// inner one it nests), which is exactly the wrong order to emit in, so this
// cannot just sort by id.
func originDeclOrder(originRHS []string) []int {
	order := make([]int, 0, len(originRHS))
	seen := make([]bool, len(originRHS))

	var visit func(id int)
	visit = func(id int) {
		if seen[id] {
			return
		}
		// Marked before recursing: an origin cannot reference itself
		// transitively (each is rendered once, and a reference can only be
		// to a var allocated during that render), but a cycle here would
		// otherwise be a stack overflow rather than a merely wrong order.
		seen[id] = true
		for _, m := range originVarRefRe.FindAllStringSubmatch(originRHS[id], -1) {
			dep, err := strconv.Atoi(m[1])
			if err != nil || dep >= len(originRHS) {
				continue
			}
			visit(dep)
		}
		order = append(order, id)
	}

	for id := range originRHS {
		visit(id)
	}
	return order
}

func renderVars(
	st *varRenderState,
	env *env,
) string {

	// Render every origin var's RHS to a string FIRST: doing so may lazily
	// register new entries into env's account/string/asset/number pools
	// (e.g. an asset literal not otherwise referenced in the program body),
	// which must be visible to the renderVar calls below — otherwise a
	// pool entry discovered only here would never get its own `type $name`
	// declaration line.
	//
	// Rendering an origin can itself declare further origin vars — an origin's
	// account position takes an arbitrary account expression, which may be
	// another balance()/meta() var — so env.originVars grows during this loop.
	// Index it rather than ranging over it: `range` fixes the length up front
	// and would leave the entries discovered here unrendered.
	var originRHS []string
	for id := 0; id < len(env.originVars); id++ {
		originRHS = append(originRHS, renderExprToString(env, env.originVars[id].origin))
	}

	st.sb.WriteString("vars {\n")
	renderVar(st, "account", env.accountsPool, accountToName, stringId)
	renderVar(st, "string", env.stringsPool, stringToName, stringId)
	renderVar(st, "asset", env.assetsPool, assetToName, stringId)
	renderVar(st, "number", env.numbersPool, numberToName, func(bi *big.Int) string {
		return bi.String()
	})
	renderVar(st, "monetary", env.monetariesPool, monetaryToName, stringId)
	renderVar(st, "portion", env.portionsPool, portionToName, stringId)
	for _, id := range originDeclOrder(originRHS) {
		st.hasVars = true
		st.sb.WriteString(indentStr)
		st.sb.WriteString(env.originVars[id].typ)
		st.sb.WriteString(" $")
		st.sb.WriteString(itemIdToName(id, "originvar"))
		st.sb.WriteString(" = ")
		st.sb.WriteString(originRHS[id])
		st.sb.WriteByte('\n')
	}
	st.sb.WriteString("}\n\n")

	if !st.hasVars {
		return ""
	}

	return st.sb.String()
}

func BuildProgram(statements ...Statement) (map[string]string, VarsEnv, string) {
	env := newEnv()
	for i, stmt := range statements {
		if i != 0 {
			env.builder.WriteString("\n\n")
		}
		stmt(&env, 0)
	}

	st := varRenderState{
		knownBindings: make(map[string]string),
	}
	// AFTER we've rendered the whole program, we can render the vars block
	vars := renderVars(&st, &env)

	return st.knownBindings, env.varsEnv, vars + env.builder.String()
}

// Check feature flag has only lower chars and "-" chars.
// Less declarative than a regex, but this way we don't need a more complex api user-wise
// just for the sake of perfs (this should be good enough)
func checkIsFlagValid(s string) bool {
	if len(s) == 0 {
		return false
	}

	for i := 0; i < len(s); i++ {
		ch := s[i]
		if (ch < 'a' || ch > 'z') && ch != '-' {
			return false
		}
	}
	return true
}

// Same as `BuildProgram`, but accepts features flag.
//
// IMPORTANT NOTE: this function will panic if a feature flag doesn't match `^[a-z-]+$`. Flags are meant to be passed directly, and a sintactically
// incorrect flag is treated as an argument error panic right away, not returned as an "error".
// Also note we don't keep the list of valid flags here
func BuildProgramWithFeatureFlags(
	featureFlags []string,
	statements ...Statement,
) (map[string]string, VarsEnv, string) {
	knownBindings, varsEnv, script := BuildProgram(statements...)

	var flagsArgs strings.Builder
	for index, flag := range featureFlags {
		if !checkIsFlagValid(flag) {
			// Yes, we are panicking instead of returning an error here.
			// That's desidered: flags are meant to be passed manually. Not computed, created conditionally, etc.
			// If a feature flag is wrong we want to crash the thing immediately instead of having the user handle that, log that, or whatever.
			panic(fmt.Sprintf("Invalid argument: the `%s` feature flag is invalid. Only flags matching `^[a-z-]+$` are accepted.", flag))
		}

		if index != 0 {
			flagsArgs.WriteString(", ")
		}

		flagsArgs.WriteByte('"')
		flagsArgs.WriteString(flag)
		flagsArgs.WriteByte('"')

	}

	updatedScript := fmt.Sprintf("#![feature(%s)]\n%s", flagsArgs.String(), script)
	return knownBindings, varsEnv, updatedScript
}
