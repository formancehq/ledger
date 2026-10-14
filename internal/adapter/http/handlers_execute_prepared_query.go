package http

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleExecutePreparedQuery handles POST /{ledgerName}/prepared-queries/{name}/execute.
func (s *Server) handleExecutePreparedQuery(w http.ResponseWriter, r *http.Request) {
	// See handleListTransactions: the profile clock started in the routing layer
	// — here the measured preparation also covers JSON body decoding and
	// parameter conversion.
	ctx := r.Context()
	profile := profileFromRequest(r)

	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	queryName := chi.URLParam(r, "queryName")
	if queryName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("query name is required"))

		return
	}

	var body struct {
		Parameters map[string]json.RawMessage `json:"parameters"`
		PageSize   uint32                     `json:"pageSize"`
		Cursor     string                     `json:"cursor"`
		Mode       string                     `json:"mode"`
		Reverse    bool                       `json:"reverse"`
	}
	// Decode the body whenever one is present. Don't gate on ContentLength
	// because chunked / unknown-length requests report ContentLength == -1;
	// io.EOF means "no body" and is the only acceptable empty case.
	if r.Body != nil {
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil && !errors.Is(err, io.EOF) {
			writeBadRequest(w, "INVALID_REQUEST", err)

			return
		}
	}

	// Also accept ?pageSize= for GET-like usage. The QS form takes precedence
	// over the JSON body when explicitly set; only parse it when the param is
	// actually present so we don't clobber a body-provided pageSize with the
	// default (parsePageSize returns DefaultPageSize for an empty QS).
	if r.URL.Query().Has("pageSize") {
		qsPageSize, ok := parsePageSize(w, r)
		if !ok {
			return
		}

		body.PageSize = qsPageSize
	}

	if c := r.URL.Query().Get("cursor"); c != "" {
		body.Cursor = c
	}

	if r.URL.Query().Has("reverse") {
		body.Reverse = queryParamBool(r, "reverse")
	}

	mode, ok := parseQueryMode(body.Mode)
	if !ok {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("unknown mode %q", body.Mode))

		return
	}

	params, err := convertJSONParameters(body.Parameters)
	if err != nil {
		writeBadRequest(w, "INVALID_PARAMETERS", err)

		return
	}

	req := &servicepb.ExecutePreparedQueryRequest{
		Ledger:     ledgerName,
		QueryName:  queryName,
		Parameters: params,
		PageSize:   body.PageSize,
		Cursor:     body.Cursor,
		Mode:       mode,
		Reverse:    body.Reverse,
	}

	profile.EnterExecute()
	resp, err := s.backend.ExecutePreparedQuery(ctx, req)
	profile.LeaveExecute()

	if err != nil {
		handleError(w, r, err)

		return
	}

	// Serialize into a clean, discriminated camelCase envelope instead of the
	// raw proto. ExecutePreparedQueryResponse has no MarshalJSON, so writing it
	// raw leaks the Go oneof shape as PascalCase `{"Result":{"Cursor":…}}` /
	// `{"Result":{"Aggregate":…}}`, violating the camelCase JSON invariant. The
	// envelope keeps the variant discriminator (`cursor` vs `aggregateResult`)
	// — the shape EN-1465 tracks — and reuses the existing correct marshalers:
	//   - aggregateResult goes through the same camelCase DTO the dedicated
	//     /aggregate handler uses (toAggregateVolumesJSON), which always emits
	//     `color` even for the uncolored bucket (the raw proto AggregatedVolume
	//     tag is `json:"color,omitempty"` and would otherwise drop it);
	//   - cursor uses the hand-written PreparedQueryCursor.MarshalJSON whose
	//     nested Account/Transaction marshalers already emit camelCase,
	//     negotiated monetary amounts, and `color` on every volume row.
	envelope := executePreparedQueryResponseJSON{}
	switch result := resp.GetResult().(type) {
	case *servicepb.ExecutePreparedQueryResponse_Aggregate:
		envelope.AggregateResult = toAggregateVolumesJSON(result.Aggregate)
	case *servicepb.ExecutePreparedQueryResponse_Cursor:
		envelope.Cursor = result.Cursor
	}

	finishProfile(w, r, profile)
	writeMonetaryJSONResponse(w, r, http.StatusOK, envelope)
}

// executePreparedQueryResponseJSON is the clean camelCase envelope for the
// prepared-query result oneof: exactly one of cursor / aggregateResult is set
// (both omitempty), replacing the leaked PascalCase proto oneof shape.
type executePreparedQueryResponseJSON struct {
	Cursor          *commonpb.PreparedQueryCursor `json:"cursor,omitempty"`
	AggregateResult *aggregateVolumesResponseJSON `json:"aggregateResult,omitempty"`
}

// convertJSONParameters converts raw JSON values into typed ParameterValue messages.
// Strings → StringValue, booleans → BoolValue, integers → Int64Value (negative)
// or Uint64Value (non-negative). A value that cannot be forwarded exactly —
// null, a fraction, or an integer outside the int64/uint64 range — is refused
// instead of being replaced by another value.
func convertJSONParameters(raw map[string]json.RawMessage) (map[string]*commonpb.ParameterValue, error) {
	if len(raw) == 0 {
		return nil, nil
	}

	params := make(map[string]*commonpb.ParameterValue, len(raw))

	for k, v := range raw {
		pv, err := jsonToParameterValue(v)
		if err != nil {
			return nil, fmt.Errorf("parameter %q: %w", k, err)
		}

		params[k] = pv
	}

	return params, nil
}

func jsonToParameterValue(raw json.RawMessage) (*commonpb.ParameterValue, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("invalid value %s: %w", string(raw), err)
	}

	switch v := v.(type) {
	case nil:
		return nil, errors.New("null is not a valid parameter value")
	case string:
		return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: v}}, nil
	case bool:
		return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_BoolValue{BoolValue: v}}, nil
	case json.Number:
		return jsonNumberToParameterValue(v)
	default:
		return nil, fmt.Errorf("unsupported value type: %s", string(raw))
	}
}

// jsonNumberToParameterValue parses n exactly (no float64 round trip), so an
// integer above 2^53 keeps every digit and one outside the int64/uint64 range
// is refused rather than wrapped. Integral notations such as 1e3 or 100.0 are
// still accepted. The fallback uses a fixed precision so a huge exponent
// costs no more than a small one.
func jsonNumberToParameterValue(n json.Number) (*commonpb.ParameterValue, error) {
	s := n.String()

	if u, err := strconv.ParseUint(s, 10, 64); err == nil {
		return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: u}}, nil
	}

	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Int64Value{Int64Value: i}}, nil
	}

	// Every int64/uint64 is exact in 128 bits: an inexact parse is either a
	// fraction or an integer far outside the range.
	f, _, err := big.ParseFloat(s, 10, 128, big.ToNearestEven)
	if err != nil {
		return nil, fmt.Errorf("invalid number %s: %w", s, err)
	}

	if f.Acc() != big.Exact || !f.IsInt() {
		if f.IsInt() {
			return nil, fmt.Errorf("integer %s is out of range", s)
		}

		return nil, fmt.Errorf("floating-point values are not supported, got %s", s)
	}

	if f.Sign() < 0 {
		if i, acc := f.Int64(); acc == big.Exact {
			return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Int64Value{Int64Value: i}}, nil
		}
	} else if u, acc := f.Uint64(); acc == big.Exact {
		return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: u}}, nil
	}

	return nil, fmt.Errorf("integer %s is out of range", s)
}

// parseQueryMode maps the wire string to a QueryMode enum value. The empty
// string defaults to LIST for backwards-compatible "no mode" callers.
func parseQueryMode(s string) (commonpb.QueryMode, bool) {
	switch s {
	case "", "LIST":
		return commonpb.QueryMode_QUERY_MODE_LIST, true
	case "AGGREGATE_VOLUMES":
		return commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES, true
	default:
		return 0, false
	}
}
