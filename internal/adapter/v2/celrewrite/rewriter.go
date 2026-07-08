// Package celrewrite implements the mirror rewrite engine applied during
// v2→v3 mirror translation.
//
// Each rule is scoped to exactly one variant of `MirrorLogEntry.data` via a
// proto oneof, which determines both the CEL type of `log` in the rule's
// `match` predicate and the set of actions the rule is allowed to carry:
// invalid combinations (e.g. `set_metadata` on a `deleted_metadata` rule)
// cannot be represented on the wire. CEL is used only for `match` — the
// actions themselves are typed proto messages executed by a Go dispatcher.
//
// Determinism is a hard invariant: rewriting runs only on the mirror leader
// and the rewritten bytes are baked into the proposed Raft order, so every
// follower applies identical bytes. The CEL environment exposes no
// non-deterministic function; every action's payload is a plain proto and the
// dispatcher is a pure function of the current log and the rule's actions.
package celrewrite

import (
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/google/cel-go/cel"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Static caps. cel.CostLimit bounds AST evaluation cost; the other caps guard
// against pathological rule sets a priori (before rules ever execute).
const (
	MaxRules       = 64
	MaxExprLen     = 4096
	MaxRegexLen    = 512
	maxEvalCost    = 1_000_000
	maxRegexCached = 256
)

// Proto message names as CEL sees them.
const (
	logType                = "raft.MirrorLogEntry"
	createdVariantType     = "raft.MirrorCreatedTransaction"
	revertedVariantType    = "raft.MirrorRevertedTransaction"
	savedMetaVariantType   = "raft.MirrorSavedMetadata"
	deletedMetaVariantType = "raft.MirrorDeletedMetadata"
)

// applyRuleFn runs a single compiled rule against a working log. It returns
// true iff the rule matched (scope + predicate) and thus applied its actions;
// callers use that to honour a `stop` flag. A false result may still mean the
// rule was compiled correctly — its scope simply didn't match this entry.
type applyRuleFn func(entry *raftcmdpb.MirrorLogEntry) (matched bool, err error)

type compiledRule struct {
	run  applyRuleFn
	stop bool
}

// Rewriter is a compiled set of mirror rewrite rules. A nil *Rewriter is a
// valid pass-through, so callers can wire it in unconditionally.
type Rewriter struct {
	rules      []compiledRule
	regexCache sync.Map // pattern string -> regexResult
}

type regexResult struct {
	re  *regexp.Regexp
	err error
}

// NewRewriter compiles a rule set. It surfaces every static error (scope not
// set, expression too long, regex not a constant, invalid metadata literal,
// bad RE2) so admission can call it to fail fast before the config reaches
// the audit chain.
func NewRewriter(rules []*commonpb.MirrorRewriteRule) (*Rewriter, error) {
	if len(rules) == 0 {
		return nil, nil
	}

	if len(rules) > MaxRules {
		return nil, fmt.Errorf("too many rewrite rules: %d (max %d)", len(rules), MaxRules)
	}

	r := &Rewriter{
		rules: make([]compiledRule, 0, len(rules)),
	}

	for i, rule := range rules {
		run, err := r.compileRule(rule)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i, err)
		}

		r.rules = append(r.rules, compiledRule{run: run, stop: rule.GetStop()})
	}

	return r, nil
}

// Apply runs the compiled rule chain against a single mirror log entry.
// A nil receiver or a fill-gap entry passes through untouched. The returned
// entry is a fresh clone: the caller's input is never mutated. When a rule
// matches with `stop=true`, no further rules run.
func (r *Rewriter) Apply(entry *raftcmdpb.MirrorLogEntry) (*raftcmdpb.MirrorLogEntry, error) {
	if r == nil || entry == nil {
		return entry, nil
	}

	// Fill-gap entries carry no addressable variant; nothing to rewrite.
	if !hasRewritableVariant(entry) {
		return entry, nil
	}

	// Clone once so the caller's log is never touched. Actions mutate this
	// working copy in place; there is no CEL-side mutation and no live-pointer
	// dependency on cel-go internals — the CEL surface is only the `match`
	// predicate, which is a pure function of the input.
	cur := cloneEntry(entry)

	for i, rule := range r.rules {
		matched, err := rule.run(cur)
		if err != nil {
			return nil, fmt.Errorf("rule %d: %w", i, err)
		}

		if matched && rule.stop {
			break
		}

		// A rule that dropped the entry replaced its data variant with FillGap;
		// further rules would have no variant to address.
		if !hasRewritableVariant(cur) {
			break
		}
	}

	if err := validateAddresses(cur); err != nil {
		return nil, err
	}

	return cur, nil
}

// compileRule dispatches on the rule's scope. Each per-variant compiler builds
// its own CEL env (with `log` typed as the specific variant), compiles the
// match predicate, and prepares an ordered list of typed action closures.
func (r *Rewriter) compileRule(rule *commonpb.MirrorRewriteRule) (applyRuleFn, error) {
	switch scope := rule.GetScope().(type) {
	case *commonpb.MirrorRewriteRule_CreatedTransaction:
		return r.compileCreatedRule(scope.CreatedTransaction)
	case *commonpb.MirrorRewriteRule_RevertedTransaction:
		return r.compileRevertedRule(scope.RevertedTransaction)
	case *commonpb.MirrorRewriteRule_SavedMetadata:
		return r.compileSavedMetadataRule(scope.SavedMetadata)
	case *commonpb.MirrorRewriteRule_DeletedMetadata:
		return r.compileDeletedMetadataRule(scope.DeletedMetadata)
	case *commonpb.MirrorRewriteRule_AnyVariant:
		return r.compileAnyVariantRule(scope.AnyVariant)
	default:
		return nil, errors.New("rule scope must be set (created_transaction | reverted_transaction | saved_metadata | deleted_metadata | any_variant)")
	}
}

// buildMatchEnv builds the CEL environment for a rule scoped to `variantType`
// (or the log itself for AnyVariant). `log` is registered as a variable of
// that type so predicates read fields directly (no need to walk through
// `log.<variant>` on a scoped rule).
func (r *Rewriter) buildMatchEnv(variantType string) (*cel.Env, error) {
	return cel.NewEnv(
		cel.Types(
			&raftcmdpb.MirrorLogEntry{},
			&raftcmdpb.MirrorCreatedTransaction{},
			&raftcmdpb.MirrorRevertedTransaction{},
			&raftcmdpb.MirrorSavedMetadata{},
			&raftcmdpb.MirrorDeletedMetadata{},
			&raftcmdpb.MirrorFillGap{},
			&commonpb.Posting{},
			&commonpb.Target{},
			&commonpb.TargetAccount{},
			&commonpb.MetadataValue{},
			&commonpb.MetadataMap{},
		),
		cel.Variable("log", cel.ObjectType(variantType)),
	)
}

// compileMatch compiles the match predicate for a rule. An empty string means
// "always matches"; anything else must be a valid CEL bool expression under
// the env. Cost is bounded by `maxEvalCost`.
func compileMatch(env *cel.Env, src string) (cel.Program, error) {
	if src == "" {
		src = "true"
	}

	if len(src) > MaxExprLen {
		return nil, fmt.Errorf("match expression too long (%d > %d)", len(src), MaxExprLen)
	}

	ast, iss := env.Compile(src)
	if iss != nil && iss.Err() != nil {
		return nil, fmt.Errorf("match compile: %w", iss.Err())
	}

	if out := ast.OutputType(); out.String() != cel.BoolType.String() {
		return nil, fmt.Errorf("match must return bool, got %s", out.String())
	}

	prog, err := env.Program(ast, cel.CostLimit(maxEvalCost))
	if err != nil {
		return nil, fmt.Errorf("match program: %w", err)
	}

	return prog, nil
}

// evalMatch runs a compiled match program against a receiver value. A runtime
// error is treated as "predicate does not apply" rather than a batch failure:
// `match` is type-checked at compile time, so a runtime failure is a
// value-shape one (typically indexing a metadata key the entry lacks). We
// suppress the error and skip the rule; stalling the mirror on a data-
// dependent predicate would be far worse.
func evalMatch(prog cel.Program, log any) (bool, error) {
	out, _, err := prog.Eval(map[string]any{"log": log})
	if err != nil {
		return false, nil //nolint:nilerr // runtime match error = rule does not apply, documented behaviour
	}

	b, ok := out.Value().(bool)
	if !ok {
		return false, fmt.Errorf("match did not return a bool (got %T)", out.Value())
	}

	return b, nil
}

// compileRegex validates and caches an RE2 pattern used by rewriteAddress /
// setAccountMetadataFromAddress actions. An empty or oversized pattern is
// rejected at admission; a syntactically invalid one likewise.
func (r *Rewriter) compileRegex(pattern string) (*regexp.Regexp, error) {
	if pattern == "" {
		return nil, errors.New("regex pattern must not be empty")
	}

	if len(pattern) > MaxRegexLen {
		return nil, fmt.Errorf("regex pattern too long (%d > %d)", len(pattern), MaxRegexLen)
	}

	if cached, ok := r.regexCache.Load(pattern); ok {
		res := cached.(regexResult)

		return res.re, res.err
	}

	re, err := regexp.Compile(pattern)
	res := regexResult{re: re, err: err}
	r.regexCache.Store(pattern, res)

	return res.re, res.err
}
