// Package celrewrite implements the CEL-based transaction rewrite engine used
// during v2->v3 mirror translation. Operators configure an ordered list of
// rules (match + cel + stop); as each v2 log is translated into a v3 mirror
// order on the (single) leader, every rule whose `match` predicate holds runs
// its `cel` rewrite against the transaction, feeding the result into the next
// rule. A rule may rename address segments, transform metadata, or drop the
// transaction entirely.
//
// Determinism is a hard invariant: rewriting runs only on the leader and the
// rewritten bytes are baked into the proposed Raft order, so every follower
// applies identical bytes (see docs/technical/architecture/mirror-cel-rewrite.md).
// The CEL environment therefore exposes no non-deterministic function (no
// wall-clock, no randomness); all helpers are pure and evaluation is bounded by
// a cost limit and by static caps enforced at compile time.
package celrewrite

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"sort"
	"sync"

	"github.com/google/cel-go/cel"
	celast "github.com/google/cel-go/common/ast"
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

	celTypeName     = "celrewrite.TxView"
	postingTypeName = "celrewrite.Posting"
)

// Entry-kind discriminants exposed to CEL as tx.type.
const (
	KindCreated        = "created"
	KindReverted       = "reverted"
	KindSetMetadata    = "setMetadata"
	KindDeleteMetadata = "deleteMetadata"
)

// Posting is the CEL-visible view of a single posting. Amount and asset are
// read-only (helpers never mutate them); only source/destination are written
// back to the proto.
type Posting struct {
	Source      string `cel:"source"`
	Destination string `cel:"destination"`
	Amount      string `cel:"amount"`
	Asset       string `cel:"asset"`
}

// TxView is the CEL-visible view of a mirror log entry. Exported fields are
// exposed to CEL (via their `cel` tag); the unexported dropped flag is set by
// tx.drop() and consumed by Apply to emit a fill-gap.
type TxView struct {
	Type            string                       `cel:"type"`
	Reference       string                       `cel:"reference"`
	Metadata        map[string]string            `cel:"metadata"`
	Postings        []Posting                    `cel:"postings"`
	AccountMetadata map[string]map[string]string `cel:"accountMetadata"`
	Target          string                       `cel:"target"`
	MetadataKey     string                       `cel:"metadataKey"`

	// targetsAccount records whether the source entry targeted an account
	// (SET_METADATA/DELETE_METADATA on an account). It is not exposed to CEL and
	// lets validation reject a target that a rule rewrote to an empty/invalid
	// address, distinct from a legitimately absent (transaction-level) target.
	targetsAccount bool

	dropped bool
}

func (v *TxView) clone() *TxView {
	nv := &TxView{
		Type:           v.Type,
		Reference:      v.Reference,
		Target:         v.Target,
		MetadataKey:    v.MetadataKey,
		targetsAccount: v.targetsAccount,
		dropped:        v.dropped,
	}

	if v.Metadata != nil {
		nv.Metadata = make(map[string]string, len(v.Metadata))
		maps.Copy(nv.Metadata, v.Metadata)
	}

	if v.Postings != nil {
		nv.Postings = make([]Posting, len(v.Postings))
		copy(nv.Postings, v.Postings)
	}

	if v.AccountMetadata != nil {
		nv.AccountMetadata = make(map[string]map[string]string, len(v.AccountMetadata))
		for acc, m := range v.AccountMetadata {
			inner := make(map[string]string, len(m))
			maps.Copy(inner, m)
			nv.AccountMetadata[acc] = inner
		}
	}

	return nv
}

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

		rewriteProg, err := r.compile(rule.GetCel(), celTxType())
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

func celTxType() *cel.Type {
	return cel.ObjectType(celTypeName)
}

func (r *Rewriter) compile(src string, want *cel.Type) (cel.Program, error) {
	ast, iss := r.env.Compile(src)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("compile error: %w", iss.Err())
	}

	if out := ast.OutputType(); out.String() != want.String() {
		return nil, fmt.Errorf("expression must evaluate to %s, got %s", want.String(), out.String())
	}

	if err := rejectViewConstruction(ast); err != nil {
		return nil, err
	}

	if err := r.validateLiteralRegexes(ast); err != nil {
		return nil, err
	}

	prog, err := r.env.Program(ast, cel.CostLimit(maxEvalCost))
	if err != nil {
		return nil, fmt.Errorf("program error: %w", err)
	}

	return prog, nil
}

// rejectViewConstruction forbids constructing the internal TxView/Posting types
// in CEL (e.g. `celrewrite.TxView{...}`). A rule must derive its result from the
// input `tx` threaded through the helper functions; a hand-built literal would
// bypass every helper guarantee — metadata validation, and the posting-count and
// account-target invariants — so it is rejected at compile/admission time. With
// construction blocked, the only TxView values in play trace back to `tx`.
func rejectViewConstruction(ast *cel.Ast) error {
	root := celast.NavigateAST(ast.NativeRep())
	structs := celast.MatchDescendants(root, func(e celast.NavigableExpr) bool {
		return e.Kind() == celast.StructKind
	})

	for _, s := range structs {
		if name := s.AsStruct().TypeName(); name == celTypeName || name == postingTypeName {
			return fmt.Errorf("constructing %s is not allowed; derive the result from tx and its helper functions", name)
		}
	}

	return nil
}

// regexHelpers are the CEL helper functions whose first argument is an RE2
// pattern (compiled by compileRegex at run time).
var regexHelpers = []string{"rewriteAddress", "setAccountMetadataFromAddress"}

// validateLiteralRegexes eagerly compiles every literal regex pattern passed to
// a regex helper in the expression, so a malformed pattern (bad RE2 or empty) is
// rejected at compile/admission time instead of stalling a mirror batch at
// runtime — restoring the fail-fast guarantee that an admitted config is safe on
// the worker. It also warms the regex cache. Non-literal (computed) patterns
// cannot be checked statically and are validated when the rule runs.
func (r *Rewriter) validateLiteralRegexes(ast *cel.Ast) error {
	root := celast.NavigateAST(ast.NativeRep())

	for _, fn := range regexHelpers {
		for _, call := range celast.MatchDescendants(root, celast.FunctionMatcher(fn)) {
			args := call.AsCall().Args()
			if len(args) == 0 {
				continue
			}

			pattern := args[0]
			if pattern.Kind() != celast.LiteralKind {
				continue
			}

			lit, ok := pattern.AsLiteral().Value().(string)
			if !ok {
				continue
			}

			if _, err := r.compileRegex(lit); err != nil {
				return fmt.Errorf("%s pattern %q: %w", fn, lit, err)
			}
		}
	}

	return nil
}

// buildEnv constructs the deterministic CEL environment: the TxView/Posting
// native types, the tx variable, ext.Strings (all deterministic), and the
// rewrite helper member functions. No non-deterministic function is registered.
func (r *Rewriter) buildEnv() (*cel.Env, error) {
	tx := celTxType()

	return cel.NewEnv(
		ext.NativeTypes(reflect.TypeFor[TxView](), reflect.TypeFor[Posting](), ext.ParseStructTag("cel")),
		ext.Strings(),
		cel.Variable("tx", tx),
		cel.Function("rewriteAddress",
			cel.MemberOverload("txview_rewriteAddress_string_string",
				[]*cel.Type{tx, cel.StringType, cel.StringType}, tx,
				cel.FunctionBinding(r.bindRewriteAddress))),
		cel.Function("setAccountMetadataFromAddress",
			cel.MemberOverload("txview_setAccountMetadataFromAddress_string_string_string",
				[]*cel.Type{tx, cel.StringType, cel.StringType, cel.StringType}, tx,
				cel.FunctionBinding(r.bindSetAccountMetadataFromAddress))),
		cel.Function("setMetadata",
			cel.MemberOverload("txview_setMetadata_string_string",
				[]*cel.Type{tx, cel.StringType, cel.StringType}, tx,
				cel.FunctionBinding(r.bindSetMetadata))),
		cel.Function("deleteMetadata",
			cel.MemberOverload("txview_deleteMetadata_string",
				[]*cel.Type{tx, cel.StringType}, tx,
				cel.FunctionBinding(r.bindDeleteMetadata))),
		cel.Function("setAccountMetadata",
			cel.MemberOverload("txview_setAccountMetadata_string_string_string",
				[]*cel.Type{tx, cel.StringType, cel.StringType, cel.StringType}, tx,
				cel.FunctionBinding(r.bindSetAccountMetadata))),
		cel.Function("deleteAccountMetadata",
			cel.MemberOverload("txview_deleteAccountMetadata_string_string",
				[]*cel.Type{tx, cel.StringType, cel.StringType}, tx,
				cel.FunctionBinding(r.bindDeleteAccountMetadata))),
		cel.Function("drop",
			cel.MemberOverload("txview_drop",
				[]*cel.Type{tx}, tx,
				cel.FunctionBinding(r.bindDrop))),
	)
}

func receiver(v ref.Val) (*TxView, ref.Val) {
	tv, ok := v.Value().(*TxView)
	if !ok {
		return nil, types.NewErr("receiver is not a transaction")
	}

	return tv, nil
}

func (r *Rewriter) bindRewriteAddress(args ...ref.Val) ref.Val {
	tv, errv := receiver(args[0])
	if tv == nil {
		return errv
	}

	pattern, ok := args[1].Value().(string)
	if !ok {
		return types.NewErr("rewriteAddress: pattern must be a string")
	}

	replacement, ok := args[2].Value().(string)
	if !ok {
		return types.NewErr("rewriteAddress: replacement must be a string")
	}

	re, err := r.compileRegex(pattern)
	if err != nil {
		return types.NewErr("rewriteAddress: %v", err)
	}

	nv := tv.clone()

	for i := range nv.Postings {
		nv.Postings[i].Source = re.ReplaceAllString(nv.Postings[i].Source, replacement)
		nv.Postings[i].Destination = re.ReplaceAllString(nv.Postings[i].Destination, replacement)
	}

	if nv.Target != "" {
		nv.Target = re.ReplaceAllString(nv.Target, replacement)
	}

	nv.AccountMetadata = rewriteAccountMetadataKeys(nv.AccountMetadata, re, replacement)

	return r.adapter.NativeToValue(nv)
}

// bindSetAccountMetadataFromAddress implements tx.setAccountMetadataFromAddress(pattern, key, replacement):
// for every posting account address matching pattern, it sets
// accountMetadata[address][key] = re.ReplaceAllString(address, replacement) —
// the mirror of rewriteAddress, used to derive per-account metadata from the
// address (e.g. capture a segment via a group and store it). Account metadata is
// only persisted for created transactions.
func (r *Rewriter) bindSetAccountMetadataFromAddress(args ...ref.Val) ref.Val {
	tv, errv := receiver(args[0])
	if tv == nil {
		return errv
	}

	pattern, ok := args[1].Value().(string)
	if !ok {
		return types.NewErr("setAccountMetadataFromAddress: pattern must be a string")
	}

	key, ok := args[2].Value().(string)
	if !ok {
		return types.NewErr("setAccountMetadataFromAddress: key must be a string")
	}

	replacement, ok := args[3].Value().(string)
	if !ok {
		return types.NewErr("setAccountMetadataFromAddress: replacement must be a string")
	}

	if err := invariants.ValidateMetadataKey(key); err != nil {
		return types.NewErr("setAccountMetadataFromAddress: invalid metadata key %q: %v", key, err)
	}

	re, err := r.compileRegex(pattern)
	if err != nil {
		return types.NewErr("setAccountMetadataFromAddress: %v", err)
	}

	nv := tv.clone()

	// Collect the unique matching posting addresses, in sorted order, so the
	// resulting map is built deterministically.
	seen := map[string]struct{}{}
	matched := make([]string, 0, len(nv.Postings)*2)

	for _, p := range nv.Postings {
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

	sort.Strings(matched)

	for _, addr := range matched {
		value := re.ReplaceAllString(addr, replacement)
		if err := invariants.ValidateMetadataString(value); err != nil {
			return types.NewErr("setAccountMetadataFromAddress: invalid metadata value %q for %q: %v", value, addr, err)
		}

		if nv.AccountMetadata == nil {
			nv.AccountMetadata = map[string]map[string]string{}
		}

		if nv.AccountMetadata[addr] == nil {
			nv.AccountMetadata[addr] = map[string]string{}
		}

		nv.AccountMetadata[addr][key] = value
	}

	return r.adapter.NativeToValue(nv)
}

func (r *Rewriter) bindSetMetadata(args ...ref.Val) ref.Val {
	tv, errv := receiver(args[0])
	if tv == nil {
		return errv
	}

	key, ok := args[1].Value().(string)
	if !ok {
		return types.NewErr("setMetadata: key must be a string")
	}

	value, ok := args[2].Value().(string)
	if !ok {
		return types.NewErr("setMetadata: value must be a string")
	}

	if errv := validateMetadataKV("setMetadata", key, value); errv != nil {
		return errv
	}

	nv := tv.clone()
	if nv.Metadata == nil {
		nv.Metadata = map[string]string{}
	}

	nv.Metadata[key] = value

	return r.adapter.NativeToValue(nv)
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

func (r *Rewriter) bindDeleteMetadata(args ...ref.Val) ref.Val {
	tv, errv := receiver(args[0])
	if tv == nil {
		return errv
	}

	key, ok := args[1].Value().(string)
	if !ok {
		return types.NewErr("deleteMetadata: key must be a string")
	}

	nv := tv.clone()
	delete(nv.Metadata, key)

	return r.adapter.NativeToValue(nv)
}

func (r *Rewriter) bindSetAccountMetadata(args ...ref.Val) ref.Val {
	tv, errv := receiver(args[0])
	if tv == nil {
		return errv
	}

	account, ok := args[1].Value().(string)
	if !ok {
		return types.NewErr("setAccountMetadata: account must be a string")
	}

	key, ok := args[2].Value().(string)
	if !ok {
		return types.NewErr("setAccountMetadata: key must be a string")
	}

	value, ok := args[3].Value().(string)
	if !ok {
		return types.NewErr("setAccountMetadata: value must be a string")
	}

	if errv := validateMetadataKV("setAccountMetadata", key, value); errv != nil {
		return errv
	}

	nv := tv.clone()
	if nv.AccountMetadata == nil {
		nv.AccountMetadata = map[string]map[string]string{}
	}

	if nv.AccountMetadata[account] == nil {
		nv.AccountMetadata[account] = map[string]string{}
	}

	nv.AccountMetadata[account][key] = value

	return r.adapter.NativeToValue(nv)
}

func (r *Rewriter) bindDeleteAccountMetadata(args ...ref.Val) ref.Val {
	tv, errv := receiver(args[0])
	if tv == nil {
		return errv
	}

	account, ok := args[1].Value().(string)
	if !ok {
		return types.NewErr("deleteAccountMetadata: account must be a string")
	}

	key, ok := args[2].Value().(string)
	if !ok {
		return types.NewErr("deleteAccountMetadata: key must be a string")
	}

	nv := tv.clone()
	if inner := nv.AccountMetadata[account]; inner != nil {
		delete(inner, key)
		if len(inner) == 0 {
			delete(nv.AccountMetadata, account)
		}
	}

	return r.adapter.NativeToValue(nv)
}

func (r *Rewriter) bindDrop(args ...ref.Val) ref.Val {
	tv, errv := receiver(args[0])
	if tv == nil {
		return errv
	}

	nv := tv.clone()
	nv.dropped = true

	return r.adapter.NativeToValue(nv)
}

// rewriteAccountMetadataKeys applies re/replacement to every account key. When
// two source accounts collapse onto the same rewritten key their maps are
// merged; iteration is over sorted source keys so the last writer on a metadata
// conflict is deterministic regardless of Go map order.
func rewriteAccountMetadataKeys(in map[string]map[string]string, re *regexp.Regexp, replacement string) map[string]map[string]string {
	if len(in) == 0 {
		return in
	}

	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make(map[string]map[string]string, len(in))
	for _, account := range keys {
		rewritten := re.ReplaceAllString(account, replacement)

		existing, ok := out[rewritten]
		if !ok {
			existing = make(map[string]string, len(in[account]))
			out[rewritten] = existing
		}

		maps.Copy(existing, in[account])
	}

	return out
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
