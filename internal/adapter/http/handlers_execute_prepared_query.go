package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// handleExecutePreparedQuery handles POST /{ledgerName}/prepared-queries/{name}/execute.
func (s *Server) handleExecutePreparedQuery(w http.ResponseWriter, r *http.Request) {
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
		Parameters     map[string]json.RawMessage `json:"parameters"`
		PageSize       uint32                     `json:"pageSize"`
		Cursor         string                     `json:"cursor"`
		MinLogSequence uint64                     `json:"minLogSequence"`
		Mode           string                     `json:"mode"`
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
		Ledger:         ledgerName,
		QueryName:      queryName,
		Parameters:     params,
		PageSize:       body.PageSize,
		Cursor:         body.Cursor,
		MinLogSequence: body.MinLogSequence,
		Mode:           mode,
	}

	ctx, profile := query.WithProfile(r.Context())

	resp, err := s.backend.ExecutePreparedQuery(ctx, req)
	if err != nil {
		handleError(w, r, err)

		return
	}

	if wantsHTTPProfile(r) {
		writeProfileHeader(w, profile)
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
	//     decimal-string amounts, and `color` on every volume row.
	envelope := executePreparedQueryResponseJSON{}
	switch result := resp.GetResult().(type) {
	case *servicepb.ExecutePreparedQueryResponse_Aggregate:
		envelope.AggregateResult = toAggregateVolumesJSON(result.Aggregate)
	case *servicepb.ExecutePreparedQueryResponse_Cursor:
		envelope.Cursor = result.Cursor
	}

	writeJSONResponse(w, http.StatusOK, envelope)
}

// executePreparedQueryResponseJSON is the clean camelCase envelope for the
// prepared-query result oneof: exactly one of cursor / aggregateResult is set
// (both omitempty), replacing the leaked PascalCase proto oneof shape.
type executePreparedQueryResponseJSON struct {
	Cursor          *commonpb.PreparedQueryCursor `json:"cursor,omitempty"`
	AggregateResult *aggregateVolumesResponseJSON `json:"aggregateResult,omitempty"`
}

// convertJSONParameters converts raw JSON values into typed ParameterValue messages.
// Strings → StringValue, booleans → BoolValue, integers → Int64Value or Uint64Value.
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
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: s}}, nil
	}

	var b bool
	if err := json.Unmarshal(raw, &b); err == nil {
		// Distinguish from number 0/1: raw must be "true" or "false"
		trimmed := string(raw)
		if trimmed == "true" || trimmed == "false" {
			return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_BoolValue{BoolValue: b}}, nil
		}
	}

	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		if f != math.Trunc(f) {
			return nil, fmt.Errorf("floating-point values are not supported, got %v", f)
		}

		if f < 0 {
			return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Int64Value{Int64Value: int64(f)}}, nil
		}

		return &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: uint64(f)}}, nil
	}

	return nil, fmt.Errorf("unsupported value type: %s", string(raw))
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
