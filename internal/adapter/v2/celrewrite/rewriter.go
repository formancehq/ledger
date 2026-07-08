// Package celrewrite implements the CEL-based transaction rewrite engine used
// during v2->v3 mirror translation. Operators configure an ordered list of
// rules (match + cel + stop); as each v2 log is translated into a v3 mirror
// order on the (single) leader, every rule whose `match` predicate holds runs
// its `cel` rewrite against the log entry, feeding the result into the next
// rule. A rule may rename address segments, transform metadata, or drop the
// entry entirely.
//
// The CEL variable is `log`: the mirror log entry as a sum type over its four
// rewritable variants (created / reverted / savedMetadata / deletedMetadata),
// each a distinct receiver type (see views.go). A helper a variant cannot
// persist is a compile-time type error, not a silent drop.
//
// Determinism is a hard invariant: rewriting runs only on the leader and the
// rewritten bytes are baked into the proposed Raft order, so every follower
// applies identical bytes (see docs/technical/architecture/subsystems/events-mirror/cel-rewrite.md).
// The CEL environment therefore exposes no non-deterministic function (no
// wall-clock, no randomness); all helpers are pure, order-sensitive map
// iteration is rejected at compile time, and evaluation is bounded by a cost
// limit and static caps.
package celrewrite

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common"
	celast "github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/ext"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Static caps (DoS guard). cel.CostLimit bounds CEL AST evaluation cost but not
// arbitrary Go work inside helper overloads, so we additionally cap the number
// of rules, the source length of each expression, and regex sizes.
const (
	MaxRules       = 64
	MaxExprLen     = 4096
	MaxRegexLen    = 512
	maxEvalCost    = 1_000_000
	maxRegexCached = 256

	logTypeName      = "celrewrite.Log"
	createdTypeName  = "celrewrite.CreatedView"
	revertedTypeName = "celrewrite.RevertedView"
	savedTypeName    = "celrewrite.SavedMetadataView"
	deletedTypeName  = "celrewrite.DeletedMetadataView"
	postingTypeName  = "celrewrite.Posting"

	// internalMapAddressApply is the private positional address-writeback the
	// mapAddress macro expands to. The '~' makes it un-typeable: the macro builds
	// the call node directly (bypassing the parser), but a rule author cannot
	// write `log.mapAddress~apply(...)` because the CEL lexer rejects '~' in a
	// function name. That structural, lexer-level guarantee keeps the raw
	// positional write off the public surface without depending on cel-go
	// AST/macro-tracking internals.
	internalMapAddressApply = "mapAddress~apply"
)

func celLogType() *cel.Type      { return cel.ObjectType(logTypeName) }
func celCreatedType() *cel.Type  { return cel.ObjectType(createdTypeName) }
func celRevertedType() *cel.Type { return cel.ObjectType(revertedTypeName) }
func celSavedType() *cel.Type    { return cel.ObjectType(savedTypeName) }

type compiledRule struct {
	match   cel.Program
	rewrite cel.Program
	stop    bool
}

// Rewriter is a compiled set of mirror rewrite rules. A nil *Rewriter is a
// valid pass-through (Apply is nil-safe), mirroring the retired AddressRewriter.
type Rewriter struct {
	rules      []compiledRule
	env        *cel.Env
	adapter    types.Adapter
	regexCache sync.Map // pattern string -> regexResult
}

type regexResult struct {
	re  *regexp.Regexp
	err error
}

// NewRewriter compiles the given rules into an executable rewriter. It returns a
// nil rewriter when there are no rules. Compilation is where all validation
// happens (expression syntax, output types, static caps), so admission can call
// NewRewriter to fail fast before a bad config reaches the audit chain.
func NewRewriter(rules []*commonpb.MirrorRewriteRule) (*Rewriter, error) {
	if len(rules) == 0 {
		return nil, nil
	}

	if len(rules) > MaxRules {
		return nil, fmt.Errorf("too many rewrite rules: %d (max %d)", len(rules), MaxRules)
	}

	r := &Rewriter{}

	env, err := r.buildEnv()
	if err != nil {
		return nil, fmt.Errorf("building CEL environment: %w", err)
	}

	r.env = env
	r.adapter = env.CELTypeAdapter()

	r.rules = make([]compiledRule, 0, len(rules))
	for i, rule := range rules {
		matchSrc := rule.GetMatch()
		if matchSrc == "" {
			matchSrc = "true"
		}

		if len(matchSrc) > MaxExprLen {
			return nil, fmt.Errorf("rule %d: match expression too long (%d > %d)", i, len(matchSrc), MaxExprLen)
		}

		if len(rule.GetCel()) > MaxExprLen {
			return nil, fmt.Errorf("rule %d: cel expression too long (%d > %d)", i, len(rule.GetCel()), MaxExprLen)
		}

		if rule.GetCel() == "" {
			return nil, fmt.Errorf("rule %d: cel expression must not be empty", i)
		}

		matchProg, err := r.compile(matchSrc, cel.BoolType)
		if err != nil {
			return nil, fmt.Errorf("rule %d: match: %w", i, err)
		}

		rewriteProg, err := r.compile(rule.GetCel(), celLogType())
		if err != nil {
			return nil, fmt.Errorf("rule %d: cel: %w", i, err)
		}

		r.rules = append(r.rules, compiledRule{
			match:   matchProg,
			rewrite: rewriteProg,
			stop:    rule.GetStop(),
		})
	}

	return r, nil
}

func (r *Rewriter) compile(src string, want *cel.Type) (cel.Program, error) {
	ast, iss := r.env.Compile(src)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("compile error: %w", iss.Err())
	}

	// Reject view construction before the output-type check so a bare
	// `celrewrite.CreatedView{}` is reported as an illegal construction rather
	// than a type mismatch.
	if err := rejectViewConstruction(ast); err != nil {
		return nil, err
	}

	if out := ast.OutputType(); out.String() != want.String() {
		return nil, fmt.Errorf("expression must evaluate to %s, got %s", want.String(), out.String())
	}

	if err := r.validateRegexPatterns(ast); err != nil {
		return nil, err
	}

	if err := validateTypeTokens(ast); err != nil {
		return nil, err
	}

	if err := validateMetadataLiterals(ast); err != nil {
		return nil, err
	}

	if err := rejectNonDeterministicMapIteration(ast); err != nil {
		return nil, err
	}

	prog, err := r.env.Program(ast, cel.CostLimit(maxEvalCost))
	if err != nil {
		return nil, fmt.Errorf("program error: %w", err)
	}

	return prog, nil
}

// viewTypeNames are the native view types a rule must not construct directly.
var viewTypeNames = map[string]struct{}{
	logTypeName:      {},
	createdTypeName:  {},
	revertedTypeName: {},
	savedTypeName:    {},
	deletedTypeName:  {},
	postingTypeName:  {},
}

// rejectViewConstruction forbids constructing the native view types in CEL (e.g.
// `celrewrite.Log{...}`). A rule must derive its result from the input `log`
// threaded through the helpers; a hand-built literal could fabricate an entry,
// set the wrong variant, or bypass the metadata/posting-count guarantees, so it
// is rejected at compile/admission time.
func rejectViewConstruction(ast *cel.Ast) error {
	root := celast.NavigateAST(ast.NativeRep())
	structs := celast.MatchDescendants(root, func(e celast.NavigableExpr) bool {
		return e.Kind() == celast.StructKind
	})

	for _, s := range structs {
		if name := s.AsStruct().TypeName(); isViewType(name) {
			return fmt.Errorf("constructing %s is not allowed; derive the result from log and its helper functions", name)
		}
	}

	return nil
}

func isViewType(name string) bool {
	_, ok := viewTypeNames[name]

	return ok
}

// regexHelpers are the CEL helper functions whose first argument is a constant
// RE2 pattern (validated and compiled at admission by validateRegexPatterns).
var regexHelpers = []string{"rewriteAddress", "setAccountMetadataFromAddress"}

// validateRegexPatterns enforces that a regex helper's pattern is a constant and
// eagerly compiles it, so a malformed pattern (bad RE2 or empty) is rejected at
// compile/admission time instead of stalling a mirror batch at run time. It also
// warms the regex cache. Requiring a constant means every pattern is validated
// up front; a computed pattern is rejected outright.
func (r *Rewriter) validateRegexPatterns(ast *cel.Ast) error {
	root := celast.NavigateAST(ast.NativeRep())

	for _, fn := range regexHelpers {
		for _, call := range celast.MatchDescendants(root, celast.FunctionMatcher(fn)) {
			args := call.AsCall().Args()
			if len(args) == 0 {
				continue
			}

			pattern := args[0]

			if pattern.Kind() != celast.LiteralKind {
				return fmt.Errorf("%s: pattern must be a constant string", fn)
			}

			lit, ok := pattern.AsLiteral().Value().(string)
			if !ok {
				return fmt.Errorf("%s: pattern must be a constant string", fn)
			}

			if _, err := r.compileRegex(lit); err != nil {
				return fmt.Errorf("%s pattern %q: %w", fn, lit, err)
			}
		}
	}

	return nil
}

// typeTokenArg maps each helper that takes an optional metadata type token to
// the argument index (excluding the receiver) of that token, so the token can be
// validated at compile time.
var typeTokenArg = map[string]int{
	"setMetadata":                   2, // key, value, type
	"setAccountMetadata":            3, // account, key, value, type
	"setAccountMetadataFromAddress": 3, // pattern, key, replacement, type
}

// validateTypeTokens enforces that the metadata type argument, when supplied, is
// a constant valid schema type. Requiring a constant means every type is fully
// checked at admission (fail-fast) rather than deferred to a run-time failure
// that would stall a mirror batch — and a computed type token has no real use.
func validateTypeTokens(ast *cel.Ast) error {
	root := celast.NavigateAST(ast.NativeRep())

	for fn, idx := range typeTokenArg {
		for _, call := range celast.MatchDescendants(root, celast.FunctionMatcher(fn)) {
			args := call.AsCall().Args()
			if len(args) <= idx {
				continue // untyped overload
			}

			token := args[idx]

			if token.Kind() != celast.LiteralKind {
				return fmt.Errorf("%s: metadata type must be a constant string", fn)
			}

			lit, ok := token.AsLiteral().Value().(string)
			if !ok {
				return fmt.Errorf("%s: metadata type must be a constant string", fn)
			}

			if _, err := commonpb.ParseMetadataType(lit); err != nil {
				return fmt.Errorf("%s type %q: %w", fn, lit, err)
			}
		}
	}

	return nil
}

// metadataKeyArg / metadataValueArg map each setter to the argument index
// (excluding the receiver) of its metadata key / value, so literal arguments can
// be validated at admission. Keys and values may be computed, so only literals
// are checked here; computed ones are validated when the rule runs.
var (
	metadataKeyArg = map[string]int{
		"setMetadata":                   0, // key, value, [type]
		"setAccountMetadata":            1, // account, key, value, [type]
		"setAccountMetadataFromAddress": 1, // pattern, key, replacement, [type]
	}
	metadataValueArg = map[string]int{
		"setMetadata":        1, // value
		"setAccountMetadata": 2, // value
		// setAccountMetadataFromAddress's value is the (computed) regex
		// replacement, so there is no literal value to check.
	}
)

// validateMetadataLiterals eagerly validates literal metadata keys and values at
// compile/admission time (same fail-fast treatment as literal regex patterns),
// so a statically-invalid helper such as log.created.setMetadata("bad key", "v")
// is rejected before the config is persisted instead of stalling a mirror batch.
func validateMetadataLiterals(ast *cel.Ast) error {
	root := celast.NavigateAST(ast.NativeRep())

	literal := func(call celast.NavigableExpr, idx int) (string, bool) {
		args := call.AsCall().Args()
		if len(args) <= idx || args[idx].Kind() != celast.LiteralKind {
			return "", false
		}

		s, ok := args[idx].AsLiteral().Value().(string)

		return s, ok
	}

	for fn, idx := range metadataKeyArg {
		for _, call := range celast.MatchDescendants(root, celast.FunctionMatcher(fn)) {
			if key, ok := literal(call, idx); ok {
				if err := invariants.ValidateMetadataKey(key); err != nil {
					return fmt.Errorf("%s: invalid metadata key %q: %w", fn, key, err)
				}
			}
		}
	}

	for fn, idx := range metadataValueArg {
		for _, call := range celast.MatchDescendants(root, celast.FunctionMatcher(fn)) {
			if value, ok := literal(call, idx); ok {
				if err := invariants.ValidateMetadataString(value); err != nil {
					return fmt.Errorf("%s: invalid metadata value %q: %w", fn, value, err)
				}
			}
		}
	}

	return nil
}

// rejectNonDeterministicMapIteration forbids order-sensitive iteration over a
// map. A CEL comprehension whose range is a map (log.created.metadata,
// log.created.accountMetadata, ...) visits keys in Go's randomized order, so a
// .map/.filter that projects those keys into a list produces a non-deterministic
// result. The rewrite must be pure — identical input yields identical bytes,
// computed once on the leader and replicated verbatim — so such an expression is
// rejected at admission. Scalar aggregations over a map (.all/.exists/
// .exists_one, which return a bool) are order-insensitive and stay allowed;
// comprehensions over an ordered list range (postings, log.addresses()) are
// deterministic and allowed (this is what log.mapAddress expands to).
func rejectNonDeterministicMapIteration(ast *cel.Ast) error {
	root := celast.NavigateAST(ast.NativeRep())

	for _, comp := range celast.MatchDescendants(root, func(e celast.NavigableExpr) bool {
		return e.Kind() == celast.ComprehensionKind
	}) {
		iterRange, ok := comp.AsComprehension().IterRange().(celast.NavigableExpr)
		if !ok {
			continue
		}

		if iterRange.Type().Kind() != types.MapKind {
			continue
		}

		if comp.Type().Kind() == types.ListKind {
			return errors.New("order-sensitive iteration over a map is not allowed: mapping or filtering a metadata map into a list is non-deterministic because map keys iterate in random order; use a scalar predicate (exists/all) or iterate an ordered list (postings)")
		}
	}

	return nil
}

// buildEnv constructs the deterministic CEL environment: the native view types,
// the `log` variable, ext.Strings/Lists/Math (all deterministic), and the
// rewrite helper member functions. No non-deterministic function is registered.
func (r *Rewriter) buildEnv() (*cel.Env, error) {
	tLog := celLogType()
	tCreated := celCreatedType()
	tReverted := celRevertedType()
	tSaved := celSavedType()
	str := cel.StringType

	return cel.NewEnv(
		ext.NativeTypes(
			reflect.TypeFor[Log](), reflect.TypeFor[CreatedView](), reflect.TypeFor[RevertedView](),
			reflect.TypeFor[SavedMetadataView](), reflect.TypeFor[DeletedMetadataView](), reflect.TypeFor[Posting](),
			ext.ParseStructTag("cel"),
		),
		ext.Strings(),
		ext.Lists(),
		ext.Math(),
		cel.Variable("log", tLog),

		// mapAddress(a, expr) maps a CEL expression over every account address in
		// the active variant. It expands to the private internalMapAddressApply
		// writeback (fed log.addresses().map(a, expr)) — see mapAddressMacro.
		cel.Macros(cel.ReceiverMacro("mapAddress", 2, mapAddressMacro)),

		cel.Function("addresses",
			cel.MemberOverload("log_addresses",
				[]*cel.Type{tLog}, cel.ListType(str),
				cel.UnaryBinding(r.bindAddresses))),
		cel.Function(internalMapAddressApply,
			cel.MemberOverload("log_mapAddressApply_list",
				[]*cel.Type{tLog, cel.ListType(str)}, tLog,
				cel.BinaryBinding(r.bindMapAddressApply))),
		cel.Function("rewriteAddress",
			cel.MemberOverload("log_rewriteAddress_string_string",
				[]*cel.Type{tLog, str, str}, tLog,
				cel.FunctionBinding(r.bindRewriteAddress))),
		cel.Function("drop",
			cel.MemberOverload("log_drop",
				[]*cel.Type{tLog}, tLog,
				cel.UnaryBinding(r.bindDrop))),

		// Functional-update wrappers: lift a transformed variant back into the
		// entry. Each accepts ONLY its variant type, so wrapping the wrong variant
		// is a compile error.
		cel.Function("withCreated",
			cel.MemberOverload("log_withCreated",
				[]*cel.Type{tLog, tCreated}, tLog,
				cel.BinaryBinding(r.bindWithCreated))),
		cel.Function("withReverted",
			cel.MemberOverload("log_withReverted",
				[]*cel.Type{tLog, tReverted}, tLog,
				cel.BinaryBinding(r.bindWithReverted))),
		cel.Function("withSavedMetadata",
			cel.MemberOverload("log_withSavedMetadata",
				[]*cel.Type{tLog, tSaved}, tLog,
				cel.BinaryBinding(r.bindWithSaved))),

		// Metadata setters/deleters, registered per variant that carries a
		// metadata field. deletedMetadata has no metadata map, so it has no
		// overload — log.deletedMetadata.setMetadata(...) is a compile error.
		cel.Function("setMetadata",
			cel.MemberOverload("created_setMetadata", []*cel.Type{tCreated, str, str}, tCreated, cel.FunctionBinding(r.bindSetMetadata)),
			cel.MemberOverload("created_setMetadata_typed", []*cel.Type{tCreated, str, str, str}, tCreated, cel.FunctionBinding(r.bindSetMetadata)),
			cel.MemberOverload("reverted_setMetadata", []*cel.Type{tReverted, str, str}, tReverted, cel.FunctionBinding(r.bindSetMetadata)),
			cel.MemberOverload("reverted_setMetadata_typed", []*cel.Type{tReverted, str, str, str}, tReverted, cel.FunctionBinding(r.bindSetMetadata)),
			cel.MemberOverload("saved_setMetadata", []*cel.Type{tSaved, str, str}, tSaved, cel.FunctionBinding(r.bindSetMetadata)),
			cel.MemberOverload("saved_setMetadata_typed", []*cel.Type{tSaved, str, str, str}, tSaved, cel.FunctionBinding(r.bindSetMetadata)),
		),
		cel.Function("deleteMetadata",
			cel.MemberOverload("created_deleteMetadata", []*cel.Type{tCreated, str}, tCreated, cel.FunctionBinding(r.bindDeleteMetadata)),
			cel.MemberOverload("reverted_deleteMetadata", []*cel.Type{tReverted, str}, tReverted, cel.FunctionBinding(r.bindDeleteMetadata)),
			cel.MemberOverload("saved_deleteMetadata", []*cel.Type{tSaved, str}, tSaved, cel.FunctionBinding(r.bindDeleteMetadata)),
		),

		// Account-metadata helpers: created only.
		cel.Function("setAccountMetadata",
			cel.MemberOverload("created_setAccountMetadata", []*cel.Type{tCreated, str, str, str}, tCreated, cel.FunctionBinding(r.bindSetAccountMetadata)),
			cel.MemberOverload("created_setAccountMetadata_typed", []*cel.Type{tCreated, str, str, str, str}, tCreated, cel.FunctionBinding(r.bindSetAccountMetadata)),
		),
		cel.Function("deleteAccountMetadata",
			cel.MemberOverload("created_deleteAccountMetadata", []*cel.Type{tCreated, str, str}, tCreated, cel.FunctionBinding(r.bindDeleteAccountMetadata)),
		),
		cel.Function("setAccountMetadataFromAddress",
			cel.MemberOverload("created_setAccountMetadataFromAddress", []*cel.Type{tCreated, str, str, str}, tCreated, cel.FunctionBinding(r.bindSetAccountMetadataFromAddress)),
			cel.MemberOverload("created_setAccountMetadataFromAddress_typed", []*cel.Type{tCreated, str, str, str, str}, tCreated, cel.FunctionBinding(r.bindSetAccountMetadataFromAddress)),
		),
	)
}

// stringArg extracts a string call argument, returning a CEL error value when it
// is not a string (should not happen given the type checker, but the binding is
// defensive).
func stringArg(args []ref.Val, i int, fn, name string) (string, ref.Val) {
	s, ok := args[i].Value().(string)
	if !ok {
		return "", types.NewErr("%s: %s must be a string", fn, name)
	}

	return s, nil
}

func (r *Rewriter) bindSetMetadata(args ...ref.Val) ref.Val {
	key, errv := stringArg(args, 1, "setMetadata", "key")
	if errv != nil {
		return errv
	}

	value, errv := stringArg(args, 2, "setMetadata", "value")
	if errv != nil {
		return errv
	}

	if errv := validateMetadataKV("setMetadata", key, value); errv != nil {
		return errv
	}

	t, typed, errv := optionalMetadataType("setMetadata", args, 3)
	if errv != nil {
		return errv
	}

	switch v := args[0].Value().(type) {
	case *CreatedView:
		nv := v.clone()
		nv.Metadata, nv.metadataTypes = setMetadataEntry(nv.Metadata, nv.metadataTypes, key, value, t, typed)

		return r.adapter.NativeToValue(nv)
	case *RevertedView:
		nv := v.clone()
		nv.Metadata, nv.metadataTypes = setMetadataEntry(nv.Metadata, nv.metadataTypes, key, value, t, typed)

		return r.adapter.NativeToValue(nv)
	case *SavedMetadataView:
		nv := v.clone()
		nv.Metadata, nv.metadataTypes = setMetadataEntry(nv.Metadata, nv.metadataTypes, key, value, t, typed)

		return r.adapter.NativeToValue(nv)
	default:
		return types.NewErr("setMetadata: unexpected receiver %T", args[0].Value())
	}
}

func (r *Rewriter) bindDeleteMetadata(args ...ref.Val) ref.Val {
	key, errv := stringArg(args, 1, "deleteMetadata", "key")
	if errv != nil {
		return errv
	}

	switch v := args[0].Value().(type) {
	case *CreatedView:
		nv := v.clone()
		nv.Metadata, nv.metadataTypes = deleteMetadataEntry(nv.Metadata, nv.metadataTypes, key)

		return r.adapter.NativeToValue(nv)
	case *RevertedView:
		nv := v.clone()
		nv.Metadata, nv.metadataTypes = deleteMetadataEntry(nv.Metadata, nv.metadataTypes, key)

		return r.adapter.NativeToValue(nv)
	case *SavedMetadataView:
		nv := v.clone()
		nv.Metadata, nv.metadataTypes = deleteMetadataEntry(nv.Metadata, nv.metadataTypes, key)

		return r.adapter.NativeToValue(nv)
	default:
		return types.NewErr("deleteMetadata: unexpected receiver %T", args[0].Value())
	}
}

func (r *Rewriter) bindSetAccountMetadata(args ...ref.Val) ref.Val {
	c, ok := args[0].Value().(*CreatedView)
	if !ok {
		return types.NewErr("setAccountMetadata: unexpected receiver %T", args[0].Value())
	}

	account, errv := stringArg(args, 1, "setAccountMetadata", "account")
	if errv != nil {
		return errv
	}

	key, errv := stringArg(args, 2, "setAccountMetadata", "key")
	if errv != nil {
		return errv
	}

	value, errv := stringArg(args, 3, "setAccountMetadata", "value")
	if errv != nil {
		return errv
	}

	if errv := validateMetadataKV("setAccountMetadata", key, value); errv != nil {
		return errv
	}

	t, typed, errv := optionalMetadataType("setAccountMetadata", args, 4)
	if errv != nil {
		return errv
	}

	nc := c.clone()
	nc.setAccountMetadata(account, key, value, t, typed)

	return r.adapter.NativeToValue(nc)
}

func (r *Rewriter) bindDeleteAccountMetadata(args ...ref.Val) ref.Val {
	c, ok := args[0].Value().(*CreatedView)
	if !ok {
		return types.NewErr("deleteAccountMetadata: unexpected receiver %T", args[0].Value())
	}

	account, errv := stringArg(args, 1, "deleteAccountMetadata", "account")
	if errv != nil {
		return errv
	}

	key, errv := stringArg(args, 2, "deleteAccountMetadata", "key")
	if errv != nil {
		return errv
	}

	nc := c.clone()
	nc.deleteAccountMetadata(account, key)

	return r.adapter.NativeToValue(nc)
}

func (r *Rewriter) bindSetAccountMetadataFromAddress(args ...ref.Val) ref.Val {
	c, ok := args[0].Value().(*CreatedView)
	if !ok {
		return types.NewErr("setAccountMetadataFromAddress: unexpected receiver %T", args[0].Value())
	}

	pattern, errv := stringArg(args, 1, "setAccountMetadataFromAddress", "pattern")
	if errv != nil {
		return errv
	}

	key, errv := stringArg(args, 2, "setAccountMetadataFromAddress", "key")
	if errv != nil {
		return errv
	}

	replacement, errv := stringArg(args, 3, "setAccountMetadataFromAddress", "replacement")
	if errv != nil {
		return errv
	}

	if err := invariants.ValidateMetadataKey(key); err != nil {
		return types.NewErr("setAccountMetadataFromAddress: invalid metadata key %q: %v", key, err)
	}

	t, typed, errv := optionalMetadataType("setAccountMetadataFromAddress", args, 4)
	if errv != nil {
		return errv
	}

	re, err := r.compileRegex(pattern)
	if err != nil {
		return types.NewErr("setAccountMetadataFromAddress: %v", err)
	}

	nc := c.clone()

	// Collect the unique matching posting addresses in sorted order, so the
	// resulting map is built deterministically.
	seen := map[string]struct{}{}
	matched := make([]string, 0, len(nc.Postings)*2)

	for _, p := range nc.Postings {
		for _, addr := range [...]string{p.Source, p.Destination} {
			if _, dup := seen[addr]; dup {
				continue
			}

			seen[addr] = struct{}{}

			if re.MatchString(addr) {
				matched = append(matched, addr)
			}
		}
	}

	sortStrings(matched)

	for _, addr := range matched {
		value := re.ReplaceAllString(addr, replacement)
		if err := invariants.ValidateMetadataString(value); err != nil {
			return types.NewErr("setAccountMetadataFromAddress: invalid metadata value %q for %q: %v", value, addr, err)
		}

		nc.setAccountMetadata(addr, key, value, t, typed)
	}

	return r.adapter.NativeToValue(nc)
}

func (r *Rewriter) bindWithCreated(logv, cv ref.Val) ref.Val {
	l, ok := logv.Value().(*Log)
	if !ok {
		return types.NewErr("withCreated: unexpected receiver %T", logv.Value())
	}

	c, ok := cv.Value().(*CreatedView)
	if !ok {
		return types.NewErr("withCreated: argument is not a created transaction")
	}

	nl := l.clone()
	nl.Created = c

	return r.adapter.NativeToValue(nl)
}

func (r *Rewriter) bindWithReverted(logv, rv ref.Val) ref.Val {
	l, ok := logv.Value().(*Log)
	if !ok {
		return types.NewErr("withReverted: unexpected receiver %T", logv.Value())
	}

	rev, ok := rv.Value().(*RevertedView)
	if !ok {
		return types.NewErr("withReverted: argument is not a reverted transaction")
	}

	nl := l.clone()
	nl.Reverted = rev

	return r.adapter.NativeToValue(nl)
}

func (r *Rewriter) bindWithSaved(logv, sv ref.Val) ref.Val {
	l, ok := logv.Value().(*Log)
	if !ok {
		return types.NewErr("withSavedMetadata: unexpected receiver %T", logv.Value())
	}

	s, ok := sv.Value().(*SavedMetadataView)
	if !ok {
		return types.NewErr("withSavedMetadata: argument is not a setMetadata op")
	}

	nl := l.clone()
	nl.SavedMetadata = s

	return r.adapter.NativeToValue(nl)
}

func (r *Rewriter) bindRewriteAddress(args ...ref.Val) ref.Val {
	l, ok := args[0].Value().(*Log)
	if !ok {
		return types.NewErr("rewriteAddress: unexpected receiver %T", args[0].Value())
	}

	pattern, errv := stringArg(args, 1, "rewriteAddress", "pattern")
	if errv != nil {
		return errv
	}

	replacement, errv := stringArg(args, 2, "rewriteAddress", "replacement")
	if errv != nil {
		return errv
	}

	re, err := r.compileRegex(pattern)
	if err != nil {
		return types.NewErr("rewriteAddress: %v", err)
	}

	return r.adapter.NativeToValue(l.rewriteAddresses(re, replacement))
}

func (r *Rewriter) bindAddresses(v ref.Val) ref.Val {
	l, ok := v.Value().(*Log)
	if !ok {
		return types.NewErr("addresses: receiver is not a log entry")
	}

	return r.adapter.NativeToValue(l.orderedAddresses())
}

func (r *Rewriter) bindMapAddressApply(recv, list ref.Val) ref.Val {
	l, ok := recv.Value().(*Log)
	if !ok {
		return types.NewErr("mapAddress: receiver is not a log entry")
	}

	native, err := list.ConvertToNative(reflect.TypeFor[[]string]())
	if err != nil {
		return types.NewErr("mapAddress: %v", err)
	}

	nl, msg := l.withAddresses(native.([]string))
	if msg != "" {
		return types.NewErr("mapAddress: %s (addresses can be transformed but not added or removed)", msg)
	}

	return r.adapter.NativeToValue(nl)
}

func (r *Rewriter) bindDrop(v ref.Val) ref.Val {
	l, ok := v.Value().(*Log)
	if !ok {
		return types.NewErr("drop: receiver is not a log entry")
	}

	nl := l.clone()
	nl.dropped = true

	return r.adapter.NativeToValue(nl)
}

// mapAddressMacro expands `log.mapAddress(a, body)` into
// `log.internalMapAddressApply(log.addresses().map(a, body))`: it maps the CEL
// expression `body` (with `a` bound to each account address) over every address
// in the active variant. This is the general, computed-address transform — e.g.
// `log.mapAddress(a, a.split(":").reverse().join(":"))` reverses the segments of
// every address, which a constant regex cannot express. The resulting addresses
// are validated at commit like any other rewrite.
//
// The writeback target (internalMapAddressApply) is a positional list-write:
// element N overwrites the Nth address in orderedAddresses order. That raw
// primitive is a footgun in the open (reordering the list silently reassigns
// addresses across postings/roles, and every output still validates), so it is
// registered under an un-typeable name — mapAddress, which can only transform
// each address in place, is the sole authoring surface. A rule author cannot
// reach the writeback: the CEL lexer rejects its name, so only this factory
// (which builds the call node directly) can produce a call to it.
func mapAddressMacro(eh cel.MacroExprFactory, target celast.Expr, args []celast.Expr) (celast.Expr, *common.Error) {
	if args[0].Kind() != celast.IdentKind {
		return nil, eh.NewError(args[0].ID(), "mapAddress: first argument must be an identifier")
	}

	iterVar := args[0].AsIdent()
	accu := eh.AccuIdentName()

	if iterVar == accu {
		return nil, eh.NewError(args[0].ID(), "mapAddress: iteration variable overwrites accumulator variable")
	}

	// Build the same comprehension the built-in `.map` macro produces, over the
	// active variant's ordered address list, then feed the result to the private
	// writeback function.
	mapped := eh.NewComprehension(
		eh.NewMemberCall("addresses", target),
		iterVar,
		accu,
		eh.NewList(),
		eh.NewLiteral(types.True),
		eh.NewCall(operators.Add, eh.NewAccuIdent(), eh.NewList(args[1])),
		eh.NewAccuIdent(),
	)

	return eh.NewMemberCall(internalMapAddressApply, target, mapped), nil
}

// optionalMetadataType resolves a metadata type token at position idx of args
// when present. It returns (type, true, nil) when a type argument is supplied,
// (0, false, nil) for the untyped overload, and an error value on a bad token.
func optionalMetadataType(fn string, args []ref.Val, idx int) (commonpb.MetadataType, bool, ref.Val) {
	if len(args) <= idx {
		return 0, false, nil
	}

	token, ok := args[idx].Value().(string)
	if !ok {
		return 0, false, types.NewErr("%s: type must be a string", fn)
	}

	t, err := commonpb.ParseMetadataType(token)
	if err != nil {
		return 0, false, types.NewErr("%s: invalid metadata type %q: %v", fn, token, err)
	}

	return t, true, nil
}

// validateMetadataKV validates a CEL-produced metadata key/value to the same
// standard as admission. Mirror-ingest orders bypass the admission metadata
// gate, so metadata a rule introduces must be checked here or an invalid key or
// value would be persisted straight into state and the audit log.
func validateMetadataKV(fn, key, value string) ref.Val {
	if err := invariants.ValidateMetadataKey(key); err != nil {
		return types.NewErr("%s: invalid metadata key %q: %v", fn, key, err)
	}

	if err := invariants.ValidateMetadataString(value); err != nil {
		return types.NewErr("%s: invalid metadata value for key %q: %v", fn, key, err)
	}

	return nil
}

func (r *Rewriter) compileRegex(pattern string) (*regexp.Regexp, error) {
	// An empty pattern matches at every boundary and would insert the
	// replacement all through the address, so reject it (the retired regex
	// rewriter did the same).
	if pattern == "" {
		return nil, errors.New("pattern must not be empty")
	}

	if len(pattern) > MaxRegexLen {
		return nil, fmt.Errorf("pattern too long (%d > %d)", len(pattern), MaxRegexLen)
	}

	if cached, ok := r.regexCache.Load(pattern); ok {
		res := cached.(regexResult)

		return res.re, res.err
	}

	re, err := regexp.Compile(pattern)

	// Bound the cache so a stream of distinct patterns can't grow it without
	// limit; determinism is unaffected since compilation is pure.
	if r.regexCacheLen() < maxRegexCached {
		r.regexCache.Store(pattern, regexResult{re: re, err: err})
	}

	return re, err
}

func (r *Rewriter) regexCacheLen() int {
	n := 0
	r.regexCache.Range(func(_, _ any) bool {
		n++

		return true
	})

	return n
}
