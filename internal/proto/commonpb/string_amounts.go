package commonpb

import "github.com/formancehq/ledger/v3/internal/adapter/json"

// This file carries the EN-1779 opt-in wire, where Posting.amount is a quoted
// decimal string instead of a bare JSON number so JavaScript clients do not
// truncate above 2^53.
//
// The mechanism: every marshaller in the posting chain owns its field list in a
// buildAux(amountsAsString bool); MarshalJSON delegates with false and is
// byte-identical to what it always emitted. Each wrapper below delegates with
// true. A parent types its child field `any` and stores the child's wrapper, so
// the mode travels down the existing encoder recursion instead of through a
// parallel type tree. An explicitly declared MarshalJSON on the wrapper takes
// precedence over the one promoted from the embedded pointer.
//
// When a retyped field carries `omitempty`, the emptiness test moves from the
// encoder to us: an interface holding a typed nil or an empty slice/map is not
// empty, so the field must be left as a nil interface unless there is
// something to emit. Guard pointers with `!= nil` and slices/maps with
// `len(x) > 0` — `!= nil` is not sufficient for a slice, because `omitempty`
// drops empty non-nil slices too. childValue and sliceValue below each make
// that guard a property of one generic function: childValue for a single
// retyped child pointer, sliceValue for a retyped slice field. wrapAll serves
// the opposite case, a slice field with no `omitempty` that must always render
// as `[]`.
//
// Every wrapper's MarshalJSON must return `[]byte("null")` when its embedded
// pointer is nil. A value wrapper over a nil pointer is not itself nil, so the
// encoder will not substitute `null` as it does for a nil *T; without the guard
// it would call MarshalJSON on the zero value and emit an object. The header
// must change the amount format only, never the response *shape*, so the guard
// is not optional at any level — the per-site comments below say only that,
// and point back here.
//
// Two marshaller shapes exist in this chain. A marshaller that always
// produces one aux struct uses buildAux(amountsAsString bool) any, as above:
// MarshalJSON delegates with false. A marshaller that dispatches a oneof and
// returns a different struct per variant — bytes, not an aux value — instead
// uses marshalStringAmounts() ([]byte, error): every non-posting-bearing
// variant delegates to the type's own MarshalJSON so both modes stay
// byte-identical there, and only the variant(s) that actually carry an
// amount build the opt-in wire themselves.
//
// Uint256.MarshalJSON is deliberately untouched: ledgerctl and misc/operator
// consume it, so the CLI wire must not move.
//
// Adding a level to the chain:
//  1. Split the type's MarshalJSON into buildAux(amountsAsString bool) any, and
//     have MarshalJSON delegate with false. If the type dispatches a oneof,
//     write marshalStringAmounts() ([]byte, error) instead and rebuild only the
//     amount-bearing variants.
//  2. Retype the child field in the aux struct to `any`.
//  3. Fill it through childValue (single child pointer) or sliceValue (slice
//     field with `omitempty`), never by hand: those hold the emptiness guard.
//  4. Declare the wrapper type here, with the nil-inner-pointer guard returning
//     `[]byte("null")` as its first statement. Export it only if a caller
//     outside this package constructs it.
//  5. Register the wrapper in the encoder-contract test so the declared
//     MarshalJSON cannot be lost to the promoted one.

// wrapAll wraps every element for the opt-in wire. The result is always
// non-nil, so a nil or empty input still marshals as `[]` rather than `null`:
// making that a property of one generic function means no per-level wrapper
// can silently reintroduce `null` via a bare `var out []W`.
func wrapAll[T, W any](items []T, wrap func(T) W) []W {
	out := make([]W, 0, len(items))
	for _, item := range items {
		out = append(out, wrap(item))
	}

	return out
}

// childValue returns a nil interface when child is nil, so a retyped `any`
// field carrying `omitempty` stays omitted instead of emitting null. Callers
// must use this at every retyped child field rather than deciding per field
// whether the guard is needed: it is safe where omitempty is absent (a nil
// interface and a nil pointer both render null there) and load-bearing where
// omitempty is present.
func childValue[T, W any](child *T, amountsAsString bool, wrap func(*T) W) any {
	if child == nil {
		return nil
	}

	if amountsAsString {
		return wrap(child)
	}

	return child
}

// sliceValue returns a nil interface when items is empty, so a retyped `any`
// field carrying `omitempty` stays omitted instead of emitting null or `[]`.
// The guard is `len(items) == 0`, not `items != nil`: `omitempty` drops an
// empty non-nil slice too, so a nil-checked guard would turn an omitted field
// into `[]`. Note this is the opposite contract to wrapAll's always-non-nil
// result: wrapAll serves Transaction.postings, which has no `omitempty` and
// must render `[]`; sliceValue serves fields that have it and must vanish.
func sliceValue[T, W any](items []T, amountsAsString bool, wrapAllOf func([]T) []W) any {
	if len(items) == 0 {
		return nil
	}

	if amountsAsString {
		return wrapAllOf(items)
	}

	return items
}

// StringAmountPosting renders a Posting with a quoted decimal amount. Exported
// because StringAmountPostings returns a slice of it, and revive's
// unexported-return rule forbids an exported function returning an unexported
// type.
type StringAmountPosting struct {
	*Posting
}

// MarshalJSON implements json.Marshaler for StringAmountPosting.
func (w StringAmountPosting) MarshalJSON() ([]byte, error) {
	// A nil inner pointer must render as `null`, not as an object: see the
	// nil-receiver rule in the file header.
	if w.Posting == nil {
		return []byte("null"), nil
	}

	return json.Marshal(w.buildAux(true))
}

// StringAmountPostings wraps each posting for the opt-in wire. The result is
// always non-nil so it marshals as `[]` rather than `null`: Transaction's
// postings field has no omitempty and the OpenAPI schema types it as a
// non-nullable required array.
func StringAmountPostings(postings []*Posting) []StringAmountPosting {
	return wrapAll(postings, func(p *Posting) StringAmountPosting {
		return StringAmountPosting{Posting: p}
	})
}

// StringAmountTransaction renders a Transaction with quoted decimal posting
// amounts. Exported because the HTTP adapter constructs it at the write sites.
type StringAmountTransaction struct {
	*Transaction
}

// MarshalJSON implements json.Marshaler for StringAmountTransaction.
func (w StringAmountTransaction) MarshalJSON() ([]byte, error) {
	// A nil inner pointer must render as `null`, not as an object: see the
	// nil-receiver rule in the file header.
	if w.Transaction == nil {
		return []byte("null"), nil
	}

	return json.Marshal(w.buildAux(true))
}

// StringAmountTransactions wraps each transaction for the opt-in wire. The
// result is always non-nil so a drained empty cursor still marshals as `[]`.
func StringAmountTransactions(transactions []*Transaction) []StringAmountTransaction {
	return wrapAll(transactions, func(tx *Transaction) StringAmountTransaction {
		return StringAmountTransaction{Transaction: tx}
	})
}

// StringAmountCreatedTransaction renders a CreatedTransaction with quoted
// decimal posting amounts on its embedded transaction.
type StringAmountCreatedTransaction struct {
	*CreatedTransaction
}

// MarshalJSON implements json.Marshaler for StringAmountCreatedTransaction.
func (w StringAmountCreatedTransaction) MarshalJSON() ([]byte, error) {
	// A nil inner pointer must render as `null`, not as an object: see the
	// nil-receiver rule in the file header.
	if w.CreatedTransaction == nil {
		return []byte("null"), nil
	}

	return json.Marshal(w.buildAux(true))
}

// StringAmountRevertedTransaction renders a RevertedTransaction with quoted
// decimal posting amounts on its embedded revert transaction.
type StringAmountRevertedTransaction struct {
	*RevertedTransaction
}

// MarshalJSON implements json.Marshaler for StringAmountRevertedTransaction.
func (w StringAmountRevertedTransaction) MarshalJSON() ([]byte, error) {
	// A nil inner pointer must render as `null`, not as an object: see the
	// nil-receiver rule in the file header.
	if w.RevertedTransaction == nil {
		return []byte("null"), nil
	}

	return json.Marshal(w.buildAux(true))
}

// stringAmountLedgerLogPayload renders a LedgerLogPayload whose posting-bearing
// variants carry quoted decimal amounts (oneof dispatch). Unexported: it is
// only ever returned
// as `any` from LedgerLog.buildAux, never from an exported function, so
// revive's unexported-return rule does not apply.
type stringAmountLedgerLogPayload struct {
	*LedgerLogPayload
}

// MarshalJSON implements json.Marshaler for stringAmountLedgerLogPayload.
func (w stringAmountLedgerLogPayload) MarshalJSON() ([]byte, error) {
	// A nil inner pointer must render as `null`, not as an object: see the
	// nil-receiver rule in the file header.
	if w.LedgerLogPayload == nil {
		return []byte("null"), nil
	}

	return w.marshalStringAmounts()
}

// stringAmountLedgerLog renders a LedgerLog with quoted decimal posting amounts
// in its payload. Unexported for the same reason as
// stringAmountLedgerLogPayload.
type stringAmountLedgerLog struct {
	*LedgerLog
}

// MarshalJSON implements json.Marshaler for stringAmountLedgerLog.
func (w stringAmountLedgerLog) MarshalJSON() ([]byte, error) {
	// A nil inner pointer must render as `null`, not as an object: see the
	// nil-receiver rule in the file header.
	if w.LedgerLog == nil {
		return []byte("null"), nil
	}

	return json.Marshal(w.buildAux(true))
}

// stringAmountApplyLedgerLog renders an ApplyLedgerLog with quoted decimal
// posting amounts in its nested ledger log. Unexported for the same reason as
// stringAmountLedgerLogPayload.
type stringAmountApplyLedgerLog struct {
	*ApplyLedgerLog
}

// MarshalJSON implements json.Marshaler for stringAmountApplyLedgerLog.
func (w stringAmountApplyLedgerLog) MarshalJSON() ([]byte, error) {
	// A nil inner pointer must render as `null`, not as an object: see the
	// nil-receiver rule in the file header.
	if w.ApplyLedgerLog == nil {
		return []byte("null"), nil
	}

	return json.Marshal(w.buildAux(true))
}

// stringAmountLogPayload renders a LogPayload whose posting-bearing variant
// carries quoted decimal amounts (oneof dispatch). Unexported for the same
// reason as stringAmountLedgerLogPayload.
type stringAmountLogPayload struct {
	*LogPayload
}

// MarshalJSON implements json.Marshaler for stringAmountLogPayload.
func (w stringAmountLogPayload) MarshalJSON() ([]byte, error) {
	// A nil inner pointer must render as `null`, not as an object: see the
	// nil-receiver rule in the file header.
	if w.LogPayload == nil {
		return []byte("null"), nil
	}

	return w.marshalStringAmounts()
}

// StringAmountLog renders a global Log with quoted decimal posting amounts
// anywhere down its payload chain. Exported because the HTTP adapter constructs
// it at the write sites.
type StringAmountLog struct {
	*Log
}

// MarshalJSON implements json.Marshaler for StringAmountLog.
func (w StringAmountLog) MarshalJSON() ([]byte, error) {
	// A nil inner pointer must render as `null`, not as an object: see the
	// nil-receiver rule in the file header.
	if w.Log == nil {
		return []byte("null"), nil
	}

	return json.Marshal(w.buildAux(true))
}

// StringAmountLogs wraps each log for the opt-in wire. The result is always
// non-nil so a drained empty cursor still marshals as `[]`.
func StringAmountLogs(logs []*Log) []StringAmountLog {
	return wrapAll(logs, func(l *Log) StringAmountLog {
		return StringAmountLog{Log: l}
	})
}

// StringAmountPreparedQueryCursor renders a PreparedQueryCursor whose
// transaction or log page carries quoted decimal posting amounts. Exported
// because the HTTP adapter constructs it at the prepared-query execute route.
type StringAmountPreparedQueryCursor struct {
	*PreparedQueryCursor
}

// MarshalJSON implements json.Marshaler for StringAmountPreparedQueryCursor.
func (w StringAmountPreparedQueryCursor) MarshalJSON() ([]byte, error) {
	// A nil inner pointer must render as `null`, not as an object: see the
	// nil-receiver rule in the file header.
	if w.PreparedQueryCursor == nil {
		return []byte("null"), nil
	}

	return json.Marshal(w.buildAux(true))
}
