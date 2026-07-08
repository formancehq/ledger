package celrewrite

import (
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Apply runs the compiled rules against a single mirror log entry and returns
// the (possibly rewritten) entry. A nil receiver is a valid pass-through. The
// entry is mutated in place only after the whole rule chain succeeds, the
// single-variant invariant holds, and all output addresses validate; a rule that
// calls log.drop() turns the entry into a fill-gap that still advances the
// transaction ID counter.
func (r *Rewriter) Apply(entry *raftcmdpb.MirrorLogEntry) (*raftcmdpb.MirrorLogEntry, error) {
	if r == nil || entry == nil {
		return entry, nil
	}

	view, ok := viewFromEntry(entry)
	if !ok {
		// Fill-gap or unknown entry kinds are passed through untouched.
		return entry, nil
	}

	cur := view
	for i, rule := range r.rules {
		out, _, err := rule.match.Eval(map[string]any{"log": cur})
		if err != nil {
			// A match that errors at runtime is treated as "does not apply" rather
			// than failing the batch. `match` is type-checked at compile time, so
			// runtime errors are value-shape errors — overwhelmingly indexing a
			// metadata key the entry doesn't have (`m["k"]` is a "no such key" error
			// in CEL, not false). Stalling the entire mirror on a data-dependent
			// predicate would be far worse than conservatively not touching this
			// entry.
			continue
		}

		matched, isBool := out.Value().(bool)
		if !isBool {
			return nil, fmt.Errorf("rule %d match did not return a bool", i)
		}

		if !matched {
			continue
		}

		res, _, err := rule.rewrite.Eval(map[string]any{"log": cur})
		if err != nil {
			return nil, fmt.Errorf("evaluating rule %d cel: %w", i, err)
		}

		nl, isLog := res.Value().(*Log)
		if !isLog {
			return nil, fmt.Errorf("rule %d cel did not return a log entry", i)
		}

		cur = nl

		if rule.stop {
			break
		}
	}

	if cur.dropped {
		return fillGapFor(entry), nil
	}

	// A rule may only transform the original variant. Unguarded access to a
	// foreign variant (has()-less) fabricates a zero view that withX would merge
	// in, so reject any result that is not exactly the source variant.
	if err := checkSingleVariant(view.kind, cur); err != nil {
		return nil, err
	}

	// targetsAccount is a property of the source entry, not of the rewritten
	// value; pin it from the original view so target validation cannot be weakened
	// by the rule chain (construction of the view types is already blocked at
	// compile time, so this is defense in depth).
	pinTargetsAccount(view, cur)

	if err := validateAddresses(cur); err != nil {
		return nil, err
	}

	if err := commitToEntry(entry, cur); err != nil {
		return nil, err
	}

	return entry, nil
}

// viewFromEntry builds the CEL-visible view of a mirror log entry with exactly
// the source variant populated. The second return is false for entry kinds the
// engine does not rewrite (fill-gap).
func viewFromEntry(entry *raftcmdpb.MirrorLogEntry) (*Log, bool) {
	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		ct := data.CreatedTransaction

		return &Log{
			kind: KindCreated,
			Created: &CreatedView{
				Reference:       ct.GetReference(),
				Postings:        postingsToView(ct.GetPostings()),
				Metadata:        metadataToStrings(ct.GetMetadata()),
				AccountMetadata: accountMetadataToStrings(ct.GetAccountMetadata()),
			},
		}, true

	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		rt := data.RevertedTransaction

		return &Log{
			kind: KindReverted,
			Reverted: &RevertedView{
				Postings: postingsToView(rt.GetReversePostings()),
				Metadata: metadataToStrings(rt.GetMetadata()),
			},
		}, true

	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		sm := data.SavedMetadata

		return &Log{
			kind: KindSetMetadata,
			SavedMetadata: &SavedMetadataView{
				Target:         targetAddr(sm.GetTarget()),
				Metadata:       metadataToStrings(sm.GetMetadata()),
				targetsAccount: isAccountTarget(sm.GetTarget()),
			},
		}, true

	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		dm := data.DeletedMetadata

		return &Log{
			kind: KindDeleteMetadata,
			DeletedMetadata: &DeletedMetadataView{
				Target:         targetAddr(dm.GetTarget()),
				Key:            dm.GetKey(),
				targetsAccount: isAccountTarget(dm.GetTarget()),
			},
		}, true

	default:
		return nil, false
	}
}

// celField maps a pinned kind to the CEL field name of its variant, for guidance
// in error messages.
func celField(kind string) string {
	switch kind {
	case KindSetMetadata:
		return "savedMetadata"
	case KindDeleteMetadata:
		return "deletedMetadata"
	default:
		return kind
	}
}

// checkSingleVariant enforces that the rewritten log carries exactly the source
// variant. A rule cannot change the entry kind, and unguarded access to a
// foreign variant fabricates a zero view (native pointer fields read as a zero
// value, not null) that withX would merge in — this catches that loudly instead
// of committing a corrupt two-variant entry.
func checkSingleVariant(kind string, l *Log) error {
	var set []string

	if l.Created != nil {
		set = append(set, KindCreated)
	}

	if l.Reverted != nil {
		set = append(set, KindReverted)
	}

	if l.SavedMetadata != nil {
		set = append(set, KindSetMetadata)
	}

	if l.DeletedMetadata != nil {
		set = append(set, KindDeleteMetadata)
	}

	if len(set) != 1 || set[0] != kind {
		return fmt.Errorf("rewrite produced variants %v but may only transform the source %s variant; guard variant access with has(log.%s)", set, kind, celField(kind))
	}

	return nil
}

func pinTargetsAccount(orig, cur *Log) {
	switch orig.kind {
	case KindSetMetadata:
		if cur.SavedMetadata != nil && orig.SavedMetadata != nil {
			cur.SavedMetadata.targetsAccount = orig.SavedMetadata.targetsAccount
		}
	case KindDeleteMetadata:
		if cur.DeletedMetadata != nil && orig.DeletedMetadata != nil {
			cur.DeletedMetadata.targetsAccount = orig.DeletedMetadata.targetsAccount
		}
	}
}

// commitToEntry writes the mutable fields of the view back onto the proto entry.
// Amounts, assets and IDs are never written. checkSingleVariant guarantees the
// committing variant is present; the nil guards are should-not-happen backstops.
func commitToEntry(entry *raftcmdpb.MirrorLogEntry, l *Log) error {
	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		c := l.Created
		if c == nil {
			return errors.New("invariant: created view missing after rewrite")
		}

		ct := data.CreatedTransaction
		if err := writeBackPostings(ct.GetPostings(), c.Postings); err != nil {
			return err
		}

		ct.Metadata = stringsToMetadata(c.Metadata, c.metadataTypes)
		ct.AccountMetadata = stringsToAccountMetadata(c.AccountMetadata, c.accountMetadataTypes)

	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		rv := l.Reverted
		if rv == nil {
			return errors.New("invariant: reverted view missing after rewrite")
		}

		rt := data.RevertedTransaction
		if err := writeBackPostings(rt.GetReversePostings(), rv.Postings); err != nil {
			return err
		}

		rt.Metadata = stringsToMetadata(rv.Metadata, rv.metadataTypes)

	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		s := l.SavedMetadata
		if s == nil {
			return errors.New("invariant: savedMetadata view missing after rewrite")
		}

		sm := data.SavedMetadata
		setTargetAddr(sm.GetTarget(), s.Target)
		sm.Metadata = stringsToMetadata(s.Metadata, s.metadataTypes)

	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		d := l.DeletedMetadata
		if d == nil {
			return errors.New("invariant: deletedMetadata view missing after rewrite")
		}

		dm := data.DeletedMetadata
		setTargetAddr(dm.GetTarget(), d.Target)
	}

	return nil
}

// fillGapFor turns a dropped entry into a fill-gap that preserves both log-ID
// contiguity and transaction-ID advancement: a dropped created/reverted
// transaction records its transaction ID in skipped_transaction_ids so the FSM
// still advances NextTransactionId and the ID can never be reused.
func fillGapFor(entry *raftcmdpb.MirrorLogEntry) *raftcmdpb.MirrorLogEntry {
	gap := &raftcmdpb.MirrorFillGap{}

	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		gap.SkippedTransactionIds = []uint64{data.CreatedTransaction.GetTransactionId()}
	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		gap.SkippedTransactionIds = []uint64{data.RevertedTransaction.GetNewTransactionId()}
	}

	return &raftcmdpb.MirrorLogEntry{
		V2LogId: entry.GetV2LogId(),
		Data: &raftcmdpb.MirrorLogEntry_FillGap{
			FillGap: gap,
		},
	}
}

// validateAddresses checks every output address of the active variant with
// invariants.ValidateLedgerAccountAddress, so a bad rewrite fails the batch
// (cursor does not advance, worker retries) rather than corrupting the mirror.
func validateAddresses(l *Log) error {
	switch {
	case l.Created != nil:
		return validatePostingsAndAccounts(l.Created.Postings, l.Created.AccountMetadata)

	case l.Reverted != nil:
		return validatePostingsAndAccounts(l.Reverted.Postings, nil)

	case l.SavedMetadata != nil && l.SavedMetadata.targetsAccount:
		return validateTarget(l.SavedMetadata.Target)

	case l.DeletedMetadata != nil && l.DeletedMetadata.targetsAccount:
		return validateTarget(l.DeletedMetadata.Target)
	}

	return nil
}

func validatePostingsAndAccounts(postings []Posting, accountMetadata map[string]map[string]string) error {
	for i := range postings {
		if err := invariants.ValidateLedgerAccountAddress(postings[i].Source); err != nil {
			return fmt.Errorf("rewritten posting source %q invalid: %w", postings[i].Source, err)
		}

		if err := invariants.ValidateLedgerAccountAddress(postings[i].Destination); err != nil {
			return fmt.Errorf("rewritten posting destination %q invalid: %w", postings[i].Destination, err)
		}
	}

	for account := range accountMetadata {
		if err := invariants.ValidateLedgerAccountAddress(account); err != nil {
			return fmt.Errorf("rewritten account-metadata address %q invalid: %w", account, err)
		}
	}

	return nil
}

// validateTarget rejects an account-targeted metadata op whose target a rule
// rewrote to an empty/invalid address (distinct from a legitimately absent,
// transaction-level target).
func validateTarget(target string) error {
	if err := invariants.ValidateLedgerAccountAddress(target); err != nil {
		return fmt.Errorf("rewritten target %q invalid: %w", target, err)
	}

	return nil
}

func postingsToView(postings []*commonpb.Posting) []Posting {
	if len(postings) == 0 {
		return nil
	}

	out := make([]Posting, len(postings))
	for i, p := range postings {
		out[i] = Posting{
			Source:      p.GetSource(),
			Destination: p.GetDestination(),
			Amount:      amountToString(p.GetAmount()),
			Asset:       p.GetAsset(),
		}
	}

	return out
}

func writeBackPostings(dst []*commonpb.Posting, src []Posting) error {
	// The view's postings map onto the proto postings by index, so a rule must
	// not change their count (helpers don't; construction is blocked at compile).
	if len(src) != len(dst) {
		return fmt.Errorf("rewrite changed posting count (%d -> %d); postings can be edited but not added or removed", len(dst), len(src))
	}

	for i := range dst {
		dst[i].Source = src[i].Source
		dst[i].Destination = src[i].Destination
	}

	return nil
}

func amountToString(u *commonpb.Uint256) string {
	if u == nil {
		return "0"
	}

	n := uint256.Int{u.GetV0(), u.GetV1(), u.GetV2(), u.GetV3()}

	return n.Dec()
}

func metadataToStrings(in map[string]*commonpb.MetadataValue) map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v.GetStringValue()
	}

	return out
}

// stringsToMetadata builds the proto metadata map, coercing each value to the
// type declared for its key (default string). Coercion follows the platform
// conversion matrix (commonpb.ConvertMetadataValue): a value that does not parse
// as the declared type becomes a null value preserving the original string.
func stringsToMetadata(in map[string]string, mdTypes map[string]commonpb.MetadataType) map[string]*commonpb.MetadataValue {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]*commonpb.MetadataValue, len(in))
	for k, v := range in {
		value := commonpb.NewStringValue(v)
		if t, ok := mdTypes[k]; ok {
			value = commonpb.ConvertMetadataValue(value, t)
		}

		out[k] = value
	}

	return out
}

func accountMetadataToStrings(in map[string]*commonpb.MetadataMap) map[string]map[string]string {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]map[string]string, len(in))
	for account, mm := range in {
		out[account] = metadataToStrings(mm.GetValues())
	}

	return out
}

func stringsToAccountMetadata(in map[string]map[string]string, mdTypes map[string]map[string]commonpb.MetadataType) map[string]*commonpb.MetadataMap {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]*commonpb.MetadataMap, len(in))
	for account, m := range in {
		out[account] = &commonpb.MetadataMap{Values: stringsToMetadata(m, mdTypes[account])}
	}

	return out
}

// isAccountTarget reports whether the target addresses an account (as opposed to
// a transaction id or being absent).
func isAccountTarget(t *commonpb.Target) bool {
	return t.GetAccount() != nil
}

func targetAddr(t *commonpb.Target) string {
	if t == nil {
		return ""
	}

	if acc := t.GetAccount(); acc != nil {
		return acc.GetAddr()
	}

	return ""
}

func setTargetAddr(t *commonpb.Target, addr string) {
	if t == nil {
		return
	}

	if acc := t.GetAccount(); acc != nil {
		acc.Addr = addr
	}
}
