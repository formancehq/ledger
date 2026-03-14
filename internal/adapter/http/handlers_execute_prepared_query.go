package http

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
)

// handleExecutePreparedQuery handles POST /{ledgerName}/prepared-queries/{name}/execute.
func (s *Server) handleExecutePreparedQuery(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")

	queryName := chi.URLParam(r, "queryName")
	if ledgerName == "" || queryName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name and query name are required"))

		return
	}

	var body struct {
		Parameters     map[string]json.RawMessage `json:"parameters"`
		PageSize       uint32                     `json:"pageSize"`
		Cursor         string                     `json:"cursor"`
		MinLogSequence uint64                     `json:"minLogSequence"`
		Mode           string                     `json:"mode"`
	}
	if r.Body != nil && r.ContentLength > 0 {
		err := json.NewDecoder(r.Body).Decode(&body)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", err)

			return
		}
	}

	// Also accept query parameters for GET-like usage
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		if v, err := strconv.ParseUint(ps, 10, 32); err == nil {
			body.PageSize = uint32(v)
		}
	}

	if c := r.URL.Query().Get("cursor"); c != "" {
		body.Cursor = c
	}

	mode := commonpb.QueryMode_QUERY_MODE_LIST
	if body.Mode == "AGGREGATE_VOLUMES" {
		mode = commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES
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

	writeJSONResponse(w, http.StatusOK, resp)
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
