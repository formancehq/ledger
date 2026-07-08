package celrewrite

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"sort"

	"github.com/google/cel-go/cel"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Per-variant compilers. Each turns a *Rule proto into an applyRuleFn that,
// at Apply time, checks the log's actual variant, evaluates the predicate,
// and runs the pre-built action closures.

func (r *Rewriter) compileCreatedRule(rule *commonpb.CreatedTransactionRule) (applyRuleFn, error) {
	env, err := r.buildMatchEnv(createdVariantType)
	if err != nil {
		return nil, fmt.Errorf("env: %w", err)
	}

	match, err := compileMatch(env, rule.GetMatch())
	if err != nil {
		return nil, err
	}

	acts, err := r.buildCreatedActions(env, rule.GetActions())
	if err != nil {
		return nil, err
	}

	return func(entry *raftcmdpb.MirrorLogEntry) (bool, error) {
		variant := entry.GetCreatedTransaction()
		if variant == nil {
			return false, nil
		}

		ok, err := evalMatch(match, variant)
		if err != nil || !ok {
			return false, err
		}

		if err := runActions(entry, acts); err != nil {
			return false, err
		}

		return true, nil
	}, nil
}

func (r *Rewriter) compileRevertedRule(rule *commonpb.RevertedTransactionRule) (applyRuleFn, error) {
	env, err := r.buildMatchEnv(revertedVariantType)
	if err != nil {
		return nil, fmt.Errorf("env: %w", err)
	}

	match, err := compileMatch(env, rule.GetMatch())
	if err != nil {
		return nil, err
	}

	acts, err := r.buildRevertedActions(env, rule.GetActions())
	if err != nil {
		return nil, err
	}

	return func(entry *raftcmdpb.MirrorLogEntry) (bool, error) {
		variant := entry.GetRevertedTransaction()
		if variant == nil {
			return false, nil
		}

		ok, err := evalMatch(match, variant)
		if err != nil || !ok {
			return false, err
		}

		if err := runActions(entry, acts); err != nil {
			return false, err
		}

		return true, nil
	}, nil
}

func (r *Rewriter) compileSavedMetadataRule(rule *commonpb.SavedMetadataRule) (applyRuleFn, error) {
	env, err := r.buildMatchEnv(savedMetaVariantType)
	if err != nil {
		return nil, fmt.Errorf("env: %w", err)
	}

	match, err := compileMatch(env, rule.GetMatch())
	if err != nil {
		return nil, err
	}

	acts, err := r.buildSavedMetadataActions(env, rule.GetActions())
	if err != nil {
		return nil, err
	}

	return func(entry *raftcmdpb.MirrorLogEntry) (bool, error) {
		variant := entry.GetSavedMetadata()
		if variant == nil {
			return false, nil
		}

		ok, err := evalMatch(match, variant)
		if err != nil || !ok {
			return false, err
		}

		if err := runActions(entry, acts); err != nil {
			return false, err
		}

		return true, nil
	}, nil
}

func (r *Rewriter) compileDeletedMetadataRule(rule *commonpb.DeletedMetadataRule) (applyRuleFn, error) {
	env, err := r.buildMatchEnv(deletedMetaVariantType)
	if err != nil {
		return nil, fmt.Errorf("env: %w", err)
	}

	match, err := compileMatch(env, rule.GetMatch())
	if err != nil {
		return nil, err
	}

	acts, err := r.buildDeletedMetadataActions(env, rule.GetActions())
	if err != nil {
		return nil, err
	}

	return func(entry *raftcmdpb.MirrorLogEntry) (bool, error) {
		variant := entry.GetDeletedMetadata()
		if variant == nil {
			return false, nil
		}

		ok, err := evalMatch(match, variant)
		if err != nil || !ok {
			return false, err
		}

		if err := runActions(entry, acts); err != nil {
			return false, err
		}

		return true, nil
	}, nil
}

func (r *Rewriter) compileAnyVariantRule(rule *commonpb.AnyVariantRule) (applyRuleFn, error) {
	env, err := r.buildMatchEnv(logType)
	if err != nil {
		return nil, fmt.Errorf("env: %w", err)
	}

	match, err := compileMatch(env, rule.GetMatch())
	if err != nil {
		return nil, err
	}

	acts, err := r.buildAnyVariantActions(env, rule.GetActions())
	if err != nil {
		return nil, err
	}

	return func(entry *raftcmdpb.MirrorLogEntry) (bool, error) {
		ok, err := evalMatch(match, entry)
		if err != nil || !ok {
			return false, err
		}

		if err := runActions(entry, acts); err != nil {
			return false, err
		}

		return true, nil
	}, nil
}

// actionFn is a prepared, typed mutation of a working log. Actions close over
// their pre-validated parameters (compiled regex, validated metadata key, …)
// so the hot path performs no reflection and no admission-time validation.
type actionFn func(*raftcmdpb.MirrorLogEntry) error

func runActions(entry *raftcmdpb.MirrorLogEntry, acts []actionFn) error {
	for i, a := range acts {
		if err := a(entry); err != nil {
			return fmt.Errorf("action %d: %w", i, err)
		}

		// A drop action replaces the entry's data with FillGap; subsequent
		// actions in the same rule would deref a nil variant, so stop here.
		if !hasRewritableVariant(entry) {
			return nil
		}
	}

	return nil
}

// -------------------- Created --------------------

func (r *Rewriter) buildCreatedActions(env *cel.Env, actions []*commonpb.CreatedTransactionAction) ([]actionFn, error) {
	out := make([]actionFn, 0, len(actions))
	for i, a := range actions {
		fn, err := r.buildCreatedAction(env, a)
		if err != nil {
			return nil, fmt.Errorf("action %d: %w", i, err)
		}

		out = append(out, fn)
	}

	return out, nil
}

func (r *Rewriter) buildCreatedAction(env *cel.Env, a *commonpb.CreatedTransactionAction) (actionFn, error) {
	switch v := a.GetAction().(type) {
	case *commonpb.CreatedTransactionAction_RewriteAddress:
		return r.actionRewriteAddress(v.RewriteAddress)
	case *commonpb.CreatedTransactionAction_SetMetadata:
		return actionSetMetadataOn(env, getCreatedMetadata, v.SetMetadata)
	case *commonpb.CreatedTransactionAction_DeleteMetadata:
		return actionDeleteMetadataOn(getCreatedMetadata, v.DeleteMetadata)
	case *commonpb.CreatedTransactionAction_SetAccountMetadata:
		return actionSetAccountMetadata(env, v.SetAccountMetadata)
	case *commonpb.CreatedTransactionAction_DeleteAccountMetadata:
		return actionDeleteAccountMetadata(v.DeleteAccountMetadata)
	case *commonpb.CreatedTransactionAction_SetAccountMetadataFromAddress:
		return r.actionSetAccountMetadataFromAddress(v.SetAccountMetadataFromAddress)
	case *commonpb.CreatedTransactionAction_Drop:
		return actionDropCreated, nil
	default:
		return nil, errors.New("unset action variant on created_transaction rule")
	}
}

// -------------------- Reverted --------------------

func (r *Rewriter) buildRevertedActions(env *cel.Env, actions []*commonpb.RevertedTransactionAction) ([]actionFn, error) {
	out := make([]actionFn, 0, len(actions))
	for i, a := range actions {
		fn, err := r.buildRevertedAction(env, a)
		if err != nil {
			return nil, fmt.Errorf("action %d: %w", i, err)
		}

		out = append(out, fn)
	}

	return out, nil
}

func (r *Rewriter) buildRevertedAction(env *cel.Env, a *commonpb.RevertedTransactionAction) (actionFn, error) {
	switch v := a.GetAction().(type) {
	case *commonpb.RevertedTransactionAction_RewriteAddress:
		return r.actionRewriteAddress(v.RewriteAddress)
	case *commonpb.RevertedTransactionAction_SetMetadata:
		return actionSetMetadataOn(env, getRevertedMetadata, v.SetMetadata)
	case *commonpb.RevertedTransactionAction_DeleteMetadata:
		return actionDeleteMetadataOn(getRevertedMetadata, v.DeleteMetadata)
	case *commonpb.RevertedTransactionAction_Drop:
		return actionDropReverted, nil
	default:
		return nil, errors.New("unset action variant on reverted_transaction rule")
	}
}

// -------------------- SavedMetadata --------------------

func (r *Rewriter) buildSavedMetadataActions(env *cel.Env, actions []*commonpb.SavedMetadataAction) ([]actionFn, error) {
	out := make([]actionFn, 0, len(actions))
	for i, a := range actions {
		fn, err := r.buildSavedMetadataAction(env, a)
		if err != nil {
			return nil, fmt.Errorf("action %d: %w", i, err)
		}

		out = append(out, fn)
	}

	return out, nil
}

func (r *Rewriter) buildSavedMetadataAction(env *cel.Env, a *commonpb.SavedMetadataAction) (actionFn, error) {
	switch v := a.GetAction().(type) {
	case *commonpb.SavedMetadataAction_RewriteAddress:
		return r.actionRewriteAddress(v.RewriteAddress)
	case *commonpb.SavedMetadataAction_SetMetadata:
		return actionSetMetadataOn(env, getSavedMetadata, v.SetMetadata)
	case *commonpb.SavedMetadataAction_DeleteMetadata:
		return actionDeleteMetadataOn(getSavedMetadata, v.DeleteMetadata)
	case *commonpb.SavedMetadataAction_Drop:
		return actionDropSimple, nil
	default:
		return nil, errors.New("unset action variant on saved_metadata rule")
	}
}

// -------------------- DeletedMetadata --------------------

func (r *Rewriter) buildDeletedMetadataActions(env *cel.Env, actions []*commonpb.DeletedMetadataAction) ([]actionFn, error) {
	out := make([]actionFn, 0, len(actions))
	for i, a := range actions {
		fn, err := r.buildDeletedMetadataAction(env, a)
		if err != nil {
			return nil, fmt.Errorf("action %d: %w", i, err)
		}

		out = append(out, fn)
	}

	return out, nil
}

func (r *Rewriter) buildDeletedMetadataAction(env *cel.Env, a *commonpb.DeletedMetadataAction) (actionFn, error) {
	switch v := a.GetAction().(type) {
	case *commonpb.DeletedMetadataAction_RewriteAddress:
		return r.actionRewriteAddress(v.RewriteAddress)
	case *commonpb.DeletedMetadataAction_Drop:
		return actionDropSimple, nil
	default:
		return nil, errors.New("unset action variant on deleted_metadata rule")
	}
}

// -------------------- AnyVariant --------------------

func (r *Rewriter) buildAnyVariantActions(env *cel.Env, actions []*commonpb.AnyVariantAction) ([]actionFn, error) {
	out := make([]actionFn, 0, len(actions))
	for i, a := range actions {
		fn, err := r.buildAnyVariantAction(env, a)
		if err != nil {
			return nil, fmt.Errorf("action %d: %w", i, err)
		}

		out = append(out, fn)
	}

	return out, nil
}

func (r *Rewriter) buildAnyVariantAction(env *cel.Env, a *commonpb.AnyVariantAction) (actionFn, error) {
	switch v := a.GetAction().(type) {
	case *commonpb.AnyVariantAction_RewriteAddress:
		return r.actionRewriteAddress(v.RewriteAddress)
	case *commonpb.AnyVariantAction_Drop:
		return actionDropCurrentVariant, nil
	default:
		return nil, errors.New("unset action variant on any_variant rule")
	}
}

// -------------------- Shared action factories --------------------

// metadataAccessor returns the metadata map slot for the log's current variant,
// creating it if nil. Callers guarantee (via variant scoping) that the entry
// carries the expected variant when the action fires.
type metadataAccessor func(entry *raftcmdpb.MirrorLogEntry) *map[string]*commonpb.MetadataValue

func getCreatedMetadata(entry *raftcmdpb.MirrorLogEntry) *map[string]*commonpb.MetadataValue {
	ct := entry.GetCreatedTransaction()
	if ct.Metadata == nil {
		ct.Metadata = map[string]*commonpb.MetadataValue{}
	}

	return &ct.Metadata
}

func getRevertedMetadata(entry *raftcmdpb.MirrorLogEntry) *map[string]*commonpb.MetadataValue {
	rt := entry.GetRevertedTransaction()
	if rt.Metadata == nil {
		rt.Metadata = map[string]*commonpb.MetadataValue{}
	}

	return &rt.Metadata
}

func getSavedMetadata(entry *raftcmdpb.MirrorLogEntry) *map[string]*commonpb.MetadataValue {
	sm := entry.GetSavedMetadata()
	if sm.Metadata == nil {
		sm.Metadata = map[string]*commonpb.MetadataValue{}
	}

	return &sm.Metadata
}

// actionSetMetadataOn builds a closure that writes (key, value) into whichever
// metadata map the accessor returns. The value comes from either a literal
// string (validated at admission) or a CEL expression compiled against `env`
// (evaluated at commit time against the current variant). An optional `type`
// token coerces the produced string into a typed MetadataValue via the
// platform conversion matrix; empty type = plain string.
func actionSetMetadataOn(env *cel.Env, accessor metadataAccessor, spec *commonpb.SetMetadataAction) (actionFn, error) {
	key := spec.GetKey()
	if err := validateKey(key); err != nil {
		return nil, fmt.Errorf("set_metadata: invalid key %q: %w", key, err)
	}

	produce, err := buildValueProducer(env, "set_metadata", key, spec.GetSource(), extractSetMetadataSource)
	if err != nil {
		return nil, err
	}

	typ, typed, err := parseOptionalMetadataType(spec.GetType())
	if err != nil {
		return nil, fmt.Errorf("set_metadata: %w", err)
	}

	return func(entry *raftcmdpb.MirrorLogEntry) error {
		value, err := produce(entry)
		if err != nil {
			return err
		}

		md := accessor(entry)
		(*md)[key] = coerceValue(value, typ, typed)

		return nil
	}, nil
}

func actionDeleteMetadataOn(accessor metadataAccessor, spec *commonpb.DeleteMetadataAction) (actionFn, error) {
	key := spec.GetKey()
	if err := validateKey(key); err != nil {
		return nil, fmt.Errorf("delete_metadata: invalid key %q: %w", key, err)
	}

	return func(entry *raftcmdpb.MirrorLogEntry) error {
		md := accessor(entry)
		delete(*md, key)

		return nil
	}, nil
}

func actionSetAccountMetadata(env *cel.Env, spec *commonpb.SetAccountMetadataAction) (actionFn, error) {
	account, key := spec.GetAccount(), spec.GetKey()
	if err := validateAccountAddress(account); err != nil {
		return nil, fmt.Errorf("set_account_metadata: invalid account %q: %w", account, err)
	}

	if err := validateKey(key); err != nil {
		return nil, fmt.Errorf("set_account_metadata: invalid key %q: %w", key, err)
	}

	produce, err := buildValueProducer(env, "set_account_metadata", key, spec.GetSource(), extractSetAccountMetadataSource)
	if err != nil {
		return nil, err
	}

	typ, typed, err := parseOptionalMetadataType(spec.GetType())
	if err != nil {
		return nil, fmt.Errorf("set_account_metadata: %w", err)
	}

	return func(entry *raftcmdpb.MirrorLogEntry) error {
		value, err := produce(entry)
		if err != nil {
			return err
		}

		ct := entry.GetCreatedTransaction()
		if ct.AccountMetadata == nil {
			ct.AccountMetadata = map[string]*commonpb.MetadataMap{}
		}

		mm, ok := ct.GetAccountMetadata()[account]
		if !ok || mm == nil {
			mm = &commonpb.MetadataMap{Values: map[string]*commonpb.MetadataValue{}}
			ct.AccountMetadata[account] = mm
		}

		if mm.Values == nil {
			mm.Values = map[string]*commonpb.MetadataValue{}
		}

		mm.Values[key] = coerceValue(value, typ, typed)

		return nil
	}, nil
}

func actionDeleteAccountMetadata(spec *commonpb.DeleteAccountMetadataAction) (actionFn, error) {
	account, key := spec.GetAccount(), spec.GetKey()
	if err := validateAccountAddress(account); err != nil {
		return nil, fmt.Errorf("delete_account_metadata: invalid account %q: %w", account, err)
	}

	if err := validateKey(key); err != nil {
		return nil, fmt.Errorf("delete_account_metadata: invalid key %q: %w", key, err)
	}

	return func(entry *raftcmdpb.MirrorLogEntry) error {
		ct := entry.GetCreatedTransaction()
		if mm, ok := ct.GetAccountMetadata()[account]; ok && mm != nil {
			delete(mm.GetValues(), key)
		}

		return nil
	}, nil
}

// compiledReplacement holds a pre-validated key + regex-replacement + optional
// typed target for one entry in a setAccountMetadataFromAddress action. The
// key is validated at admission; the produced string is re-validated at commit
// (it's computed from the address).
type compiledReplacement struct {
	key         string
	replacement string
	typ         commonpb.MetadataType
	typed       bool
}

func (r *Rewriter) actionSetAccountMetadataFromAddress(spec *commonpb.SetAccountMetadataFromAddressAction) (actionFn, error) {
	re, err := r.compileRegex(spec.GetPattern())
	if err != nil {
		return nil, fmt.Errorf("set_account_metadata_from_address: %w", err)
	}

	if len(spec.GetReplacements()) == 0 {
		return nil, errors.New("set_account_metadata_from_address: at least one replacement is required")
	}

	reps := make([]compiledReplacement, 0, len(spec.GetReplacements()))

	for i, rep := range spec.GetReplacements() {
		key := rep.GetKey()
		if err := validateKey(key); err != nil {
			return nil, fmt.Errorf("set_account_metadata_from_address: replacement %d: invalid key %q: %w", i, key, err)
		}

		typ, typed, err := parseOptionalMetadataType(rep.GetType())
		if err != nil {
			return nil, fmt.Errorf("set_account_metadata_from_address: replacement %d: %w", i, err)
		}

		reps = append(reps, compiledReplacement{
			key:         key,
			replacement: rep.GetReplacement(),
			typ:         typ,
			typed:       typed,
		})
	}

	return func(entry *raftcmdpb.MirrorLogEntry) error {
		ct := entry.GetCreatedTransaction()

		// Sorted iteration so collisions merge deterministically.
		seen := map[string]struct{}{}
		for _, p := range ct.GetPostings() {
			if p.GetSource() != "" {
				seen[p.GetSource()] = struct{}{}
			}

			if p.GetDestination() != "" {
				seen[p.GetDestination()] = struct{}{}
			}
		}

		matched := make([]string, 0, len(seen))

		for addr := range seen {
			if re.MatchString(addr) {
				matched = append(matched, addr)
			}
		}

		sort.Strings(matched)

		if ct.AccountMetadata == nil {
			ct.AccountMetadata = map[string]*commonpb.MetadataMap{}
		}

		for _, addr := range matched {
			mm, ok := ct.GetAccountMetadata()[addr]
			if !ok || mm == nil {
				mm = &commonpb.MetadataMap{Values: map[string]*commonpb.MetadataValue{}}
				ct.AccountMetadata[addr] = mm
			}

			if mm.Values == nil {
				mm.Values = map[string]*commonpb.MetadataValue{}
			}

			for _, rep := range reps {
				value := re.ReplaceAllString(addr, rep.replacement)
				if err := validateValue(value); err != nil {
					return fmt.Errorf("set_account_metadata_from_address: replacement %q produced invalid value %q for %q: %w", rep.key, value, addr, err)
				}

				sv := commonpb.NewStringValue(value)
				if rep.typed {
					sv = commonpb.ConvertMetadataValue(sv, rep.typ)
				}

				mm.Values[rep.key] = sv
			}
		}

		return nil
	}, nil
}

// actionRewriteAddress builds a closure that rewrites every account address
// slot on the entry, regardless of variant. Which slots exist depends on the
// variant at Apply time.
func (r *Rewriter) actionRewriteAddress(spec *commonpb.RewriteAddressAction) (actionFn, error) {
	pattern, replacement := spec.GetPattern(), spec.GetReplacement()

	re, err := r.compileRegex(pattern)
	if err != nil {
		return nil, fmt.Errorf("rewrite_address: %w", err)
	}

	return func(entry *raftcmdpb.MirrorLogEntry) error {
		switch data := entry.GetData().(type) {
		case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
			rewritePostings(re, replacement, data.CreatedTransaction.GetPostings())
			data.CreatedTransaction.AccountMetadata = rewriteAccountMetadataKeys(re, replacement, data.CreatedTransaction.GetAccountMetadata())
		case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
			rewritePostings(re, replacement, data.RevertedTransaction.GetReversePostings())
		case *raftcmdpb.MirrorLogEntry_SavedMetadata:
			rewriteTargetAddr(re, replacement, data.SavedMetadata.GetTarget())
		case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
			rewriteTargetAddr(re, replacement, data.DeletedMetadata.GetTarget())
		}

		return nil
	}, nil
}

// Drop implementations. Each turns the current entry into a FillGap in place;
// tx-id-carrying variants (created, reverted) record the source ID in
// SkippedTransactionIds so the FSM still advances NextTransactionId.

func actionDropCreated(entry *raftcmdpb.MirrorLogEntry) error {
	gap := &raftcmdpb.MirrorFillGap{
		SkippedTransactionIds: []uint64{entry.GetCreatedTransaction().GetTransactionId()},
	}
	entry.Data = &raftcmdpb.MirrorLogEntry_FillGap{FillGap: gap}

	return nil
}

func actionDropReverted(entry *raftcmdpb.MirrorLogEntry) error {
	gap := &raftcmdpb.MirrorFillGap{
		SkippedTransactionIds: []uint64{entry.GetRevertedTransaction().GetNewTransactionId()},
	}
	entry.Data = &raftcmdpb.MirrorLogEntry_FillGap{FillGap: gap}

	return nil
}

func actionDropSimple(entry *raftcmdpb.MirrorLogEntry) error {
	entry.Data = &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{}}

	return nil
}

func actionDropCurrentVariant(entry *raftcmdpb.MirrorLogEntry) error {
	// Cross-variant drop: choose the right sentinel based on the entry's
	// current data variant.
	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		entry.Data = &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{
			SkippedTransactionIds: []uint64{data.CreatedTransaction.GetTransactionId()},
		}}
	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		entry.Data = &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{
			SkippedTransactionIds: []uint64{data.RevertedTransaction.GetNewTransactionId()},
		}}
	default:
		entry.Data = &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{}}
	}

	return nil
}

// -------------------- Address rewrite primitives --------------------

// rewritePostings mutates every posting's source/destination in place.
func rewritePostings(re *regexp.Regexp, replacement string, postings []*commonpb.Posting) {
	for _, p := range postings {
		if p == nil {
			continue
		}

		p.Source = re.ReplaceAllString(p.GetSource(), replacement)
		p.Destination = re.ReplaceAllString(p.GetDestination(), replacement)
	}
}

// rewriteTargetAddr rewrites the address on a Target (no-op for TransactionId
// targets or nil).
func rewriteTargetAddr(re *regexp.Regexp, replacement string, t *commonpb.Target) {
	if t == nil {
		return
	}

	if acc := t.GetAccount(); acc != nil {
		acc.Addr = re.ReplaceAllString(acc.GetAddr(), replacement)
	}
}

// rewriteAccountMetadataKeys re-keys every entry by applying the replacement.
// Sorted iteration + last-writer-wins on the inner value map keeps merges
// deterministic when two rewritten keys collapse.
func rewriteAccountMetadataKeys(re *regexp.Regexp, replacement string, in map[string]*commonpb.MetadataMap) map[string]*commonpb.MetadataMap {
	if len(in) == 0 {
		return in
	}

	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	out := make(map[string]*commonpb.MetadataMap, len(in))

	for _, k := range keys {
		newKey := re.ReplaceAllString(k, replacement)
		src := in[k]

		existing, ok := out[newKey]
		if !ok || existing == nil {
			out[newKey] = &commonpb.MetadataMap{Values: cloneMetadataValues(src.GetValues())}

			continue
		}

		if existing.Values == nil {
			existing.Values = map[string]*commonpb.MetadataValue{}
		}

		maps.Copy(existing.GetValues(), src.GetValues())
	}

	return out
}

func cloneMetadataValues(in map[string]*commonpb.MetadataValue) map[string]*commonpb.MetadataValue {
	if in == nil {
		return nil
	}

	out := make(map[string]*commonpb.MetadataValue, len(in))
	maps.Copy(out, in)

	return out
}
