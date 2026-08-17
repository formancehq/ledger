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
// drops empty non-nil slices too. childValue below makes that guard a
// property of one generic function for the common case of a single retyped
// child pointer, the same way wrapAll does for slices.
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

// StringAmountPosting renders a Posting with a quoted decimal amount. Exported
// because StringAmountPostings returns a slice of it, and revive's
// unexported-return rule forbids an exported function returning an unexported
// type.
type StringAmountPosting struct {
	*Posting
}

// MarshalJSON implements json.Marshaler for StringAmountPosting.
func (w StringAmountPosting) MarshalJSON() ([]byte, error) {
	// A value wrapper over a nil pointer is not nil, so the encoder will not
	// substitute `null` for us as it does for a nil *Posting. Match the default
	// wire explicitly.
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
	// A value wrapper over a nil pointer is not nil, so the encoder will not
	// substitute `null` for us as it does for a nil *Transaction. Match the
	// default wire explicitly.
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
	// A value wrapper over a nil pointer is not nil, so the encoder will not
	// substitute `null` for us as it does for a nil *CreatedTransaction. Match
	// the default wire explicitly.
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
	// A value wrapper over a nil pointer is not nil, so the encoder will not
	// substitute `null` for us as it does for a nil *RevertedTransaction. Match
	// the default wire explicitly.
	if w.RevertedTransaction == nil {
		return []byte("null"), nil
	}

	return json.Marshal(w.buildAux(true))
}
