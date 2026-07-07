package celrewrite

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/formancehq/invariants"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Apply runs the compiled rules against a single mirror log entry and returns
// the (possibly rewritten) entry. A nil receiver is a valid pass-through. The
// entry is mutated in place only after the whole rule chain succeeds and all
// output addresses validate; a rule that calls tx.drop() turns the entry into a
// fill-gap that still advances the transaction ID counter.
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
		out, _, err := rule.match.Eval(map[string]any{"tx": cur})
		if err != nil {
			// A match that errors at runtime is treated as "does not apply"
			// rather than failing the batch. `match` is type-checked at compile
			// time, so runtime errors are value-shape errors — overwhelmingly
			// indexing a metadata key the transaction doesn't have
			// (`tx.metadata["k"]` is a "no such key" error in CEL, not false).
			// Stalling the entire mirror on a data-dependent predicate would be
			// far worse than conservatively not touching this transaction.
			continue
		}

		matched, isBool := out.Value().(bool)
		if !isBool {
			return nil, fmt.Errorf("rule %d match did not return a bool", i)
		}

		if !matched {
			continue
		}

		res, _, err := rule.rewrite.Eval(map[string]any{"tx": cur})
		if err != nil {
			return nil, fmt.Errorf("evaluating rule %d cel: %w", i, err)
		}

		nv, isTx := res.Value().(*TxView)
		if !isTx {
			return nil, fmt.Errorf("rule %d cel did not return a transaction", i)
		}

		cur = nv

		if rule.stop {
			break
		}
	}

	if cur.dropped {
		return fillGapFor(entry), nil
	}

	// targetsAccount is a property of the source entry, not of the rewritten
	// value. Pin it from the original view so target validation cannot be
	// weakened by the rule chain (construction of TxView is already blocked at
	// compile time, so this is defense in depth).
	cur.targetsAccount = view.targetsAccount

	if err := validateAddresses(cur); err != nil {
		return nil, err
	}

	if err := commitToEntry(entry, cur); err != nil {
		return nil, err
	}

	return entry, nil
}

// viewFromEntry builds the CEL-visible view of a mirror log entry. The second
// return is false for entry kinds the engine does not rewrite (fill-gap).
func viewFromEntry(entry *raftcmdpb.MirrorLogEntry) (*TxView, bool) {
	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		ct := data.CreatedTransaction

		return &TxView{
			Type:            KindCreated,
			Reference:       ct.GetReference(),
			Metadata:        metadataToStrings(ct.GetMetadata()),
			Postings:        postingsToView(ct.GetPostings()),
			AccountMetadata: accountMetadataToStrings(ct.GetAccountMetadata()),
		}, true

	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		rt := data.RevertedTransaction

		return &TxView{
			Type:     KindReverted,
			Metadata: metadataToStrings(rt.GetMetadata()),
			Postings: postingsToView(rt.GetReversePostings()),
		}, true

	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		sm := data.SavedMetadata

		return &TxView{
			Type:           KindSetMetadata,
			Metadata:       metadataToStrings(sm.GetMetadata()),
			Target:         targetAddr(sm.GetTarget()),
			targetsAccount: isAccountTarget(sm.GetTarget()),
		}, true

	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		dm := data.DeletedMetadata

		return &TxView{
			Type:           KindDeleteMetadata,
			Target:         targetAddr(dm.GetTarget()),
			MetadataKey:    dm.GetKey(),
			targetsAccount: isAccountTarget(dm.GetTarget()),
		}, true

	default:
		return nil, false
	}
}

// commitToEntry writes the mutable fields of the view back onto the proto entry.
// Amounts, assets and IDs are never written (read-only in the view). Only
// posting source/destination are copied, by index — so the view's posting count
// must match the entry's. Helpers preserve the count, but a rule that returns a
// hand-built TxView literal could change it; that is rejected here rather than
// silently mis-aligning addresses with the wrong amounts.
func commitToEntry(entry *raftcmdpb.MirrorLogEntry, v *TxView) error {
	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		ct := data.CreatedTransaction
		if err := writeBackPostings(ct.GetPostings(), v.Postings); err != nil {
			return err
		}

		ct.Metadata = stringsToMetadata(v.Metadata)
		ct.AccountMetadata = stringsToAccountMetadata(v.AccountMetadata)

	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		rt := data.RevertedTransaction
		if err := writeBackPostings(rt.GetReversePostings(), v.Postings); err != nil {
			return err
		}

		rt.Metadata = stringsToMetadata(v.Metadata)

	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		sm := data.SavedMetadata
		setTargetAddr(sm.GetTarget(), v.Target)
		sm.Metadata = stringsToMetadata(v.Metadata)

	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		dm := data.DeletedMetadata
		setTargetAddr(dm.GetTarget(), v.Target)
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

func validateAddresses(v *TxView) error {
	for i := range v.Postings {
		if err := invariants.ValidateLedgerAccountAddress(v.Postings[i].Source); err != nil {
			return fmt.Errorf("rewritten posting source %q invalid: %w", v.Postings[i].Source, err)
		}

		if err := invariants.ValidateLedgerAccountAddress(v.Postings[i].Destination); err != nil {
			return fmt.Errorf("rewritten posting destination %q invalid: %w", v.Postings[i].Destination, err)
		}
	}

	// An account-targeted metadata op must keep a valid address. Gate on the
	// original entry (targetsAccount), not on whether Target is now non-empty:
	// a rule that rewrote the account target to "" must be rejected, not silently
	// treated as a transaction-level (no-account) target.
	if v.targetsAccount {
		if err := invariants.ValidateLedgerAccountAddress(v.Target); err != nil {
			return fmt.Errorf("rewritten target %q invalid: %w", v.Target, err)
		}
	}

	for account := range v.AccountMetadata {
		if err := invariants.ValidateLedgerAccountAddress(account); err != nil {
			return fmt.Errorf("rewritten account-metadata address %q invalid: %w", account, err)
		}
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
	// not change their count (helpers don't; a hand-built TxView literal could).
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

func stringsToMetadata(in map[string]string) map[string]*commonpb.MetadataValue {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]*commonpb.MetadataValue, len(in))
	for k, v := range in {
		out[k] = &commonpb.MetadataValue{
			Type: &commonpb.MetadataValue_StringValue{StringValue: v},
		}
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

func stringsToAccountMetadata(in map[string]map[string]string) map[string]*commonpb.MetadataMap {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]*commonpb.MetadataMap, len(in))
	for account, m := range in {
		out[account] = &commonpb.MetadataMap{Values: stringsToMetadata(m)}
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
