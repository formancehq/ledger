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
// drops empty non-nil slices too.
//
// Uint256.MarshalJSON is deliberately untouched: ledgerctl and misc/operator
// consume it, so the CLI wire must not move.

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
	out := make([]StringAmountPosting, 0, len(postings))
	for _, p := range postings {
		out = append(out, StringAmountPosting{Posting: p})
	}

	return out
}
