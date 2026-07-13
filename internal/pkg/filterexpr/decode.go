package filterexpr

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// DecodeDualFormat parses a filter supplied in EITHER of the two supported
// representations into the SAME *commonpb.QueryFilter, then runs the single
// per-target validity gate (domain.ValidateFilterForTarget) on the result. It is
// the one entry point every filtered surface (list endpoints, prepared queries,
// audit reads, ledgerctl --filter) uses, so a caller never has to know which
// syntax an endpoint "wants": both are accepted interchangeably and compile
// through the same downstream path (EN-1511).
//
// The two representations are:
//
//   - structured — the v2 QueryFilter JSON DSL ($and/$or/$not + $match/$gt/…),
//     decoded by commonpb.QueryFilter.UnmarshalJSON (query_filter.go);
//   - textual    — the human-readable filterexpr grammar (metadata[k] == v,
//     audit[outcome] == failure, …), parsed by Parse.
//
// Form detection is purely structural and does not depend on the transport: the
// first non-whitespace byte of the raw value decides. A leading '{' is the
// structured JSON DSL (every DSL node is a JSON object). A leading '"' is a
// JSON-quoted string whose contents are textual filterexpr — this lets a JSON
// body field carry either form (`"filter": {"$match": …}` or
// `"filter": "metadata[k] == v"`). Anything else is treated as raw textual
// filterexpr, which is how a query-string value (`?filter=metadata[k]==v`)
// arrives. A caller passing the structured form over a query string simply
// URL-encodes the JSON object as the value.
//
// AUDIT is text-only: an audit condition has no structured JSON representation
// (the field-dispatched DSL cannot round-trip audit field names — ledger,
// timestamp, … — without colliding with the transaction/log conditions that
// already claim them; see commonpb/query_filter.go, EN-1241). This is not a
// special case here: the structured decoder rejects audit conditions on its own,
// so a structured-form audit filter fails with that codec's error, while the
// textual form parses the audit arm natively. Callers that read AUDIT should
// document the textual form as the canonical one.
//
// A nil/empty raw value yields (nil, nil): "no filter" is a valid unfiltered
// read for the list endpoints. Callers that require a filter (prepared queries)
// check for nil themselves — the same contract the previous per-endpoint
// decoders had.
func DecodeDualFormat(raw []byte, target commonpb.QueryTarget) (*commonpb.QueryFilter, error) {
	filter, err := decodeDualFormat(raw)
	if err != nil {
		return nil, err
	}

	if filter == nil {
		return nil, nil
	}

	if verr := domain.ValidateFilterForTarget(filter, target); verr != nil {
		return nil, verr
	}

	return filter, nil
}

// DecodeDualFormatStructuralOnly is DecodeDualFormat without the per-target
// validity gate. It exists for the prepared-query update path, which does not
// see the target (the target is immutable and lives on the stored query, so the
// FSM applies the gate against it — see handlers_update_prepared_query.go). Every
// other caller MUST use DecodeDualFormat so form and validity are checked in one
// place.
func DecodeDualFormatStructuralOnly(raw []byte) (*commonpb.QueryFilter, error) {
	return decodeDualFormat(raw)
}

// decodeDualFormat performs form detection + parse, without the validity gate.
func decodeDualFormat(raw []byte) (*commonpb.QueryFilter, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || string(trimmed) == "null" {
		return nil, nil
	}

	switch trimmed[0] {
	case '{':
		// Structured v2 QueryFilter JSON DSL.
		filter := &commonpb.QueryFilter{}
		if err := json.Unmarshal(trimmed, filter); err != nil {
			return nil, fmt.Errorf("filter: %w", err)
		}

		// Defensive: the codec already rejects a structurally-empty object (`{}`
		// fails with "empty object"), so a successful unmarshal normally populates
		// the oneof. Guard the residual case where it does not, rather than passing
		// an empty filter downstream (which would fail later at execute time with
		// "unknown filter type: <nil>").
		if filter.GetFilter() == nil {
			return nil, errors.New("filter must contain at least one condition")
		}

		return filter, nil

	case '"':
		// JSON-quoted string carrying textual filterexpr (body-field form).
		var expr string
		if err := json.Unmarshal(trimmed, &expr); err != nil {
			return nil, fmt.Errorf("filter: %w", err)
		}

		return parseTextual(expr)

	default:
		// Raw textual filterexpr (query-string form).
		return parseTextual(string(trimmed))
	}
}

// parseTextual parses a textual filterexpr expression, treating an empty string
// as "no filter" for symmetry with the empty-raw case (a body field of `""` or a
// query param of `?filter=` is an explicit no-op, not a parse error).
func parseTextual(expr string) (*commonpb.QueryFilter, error) {
	if expr == "" {
		return nil, nil
	}

	filter, err := Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}

	return filter, nil
}
