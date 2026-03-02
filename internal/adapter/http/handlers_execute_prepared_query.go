package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

// handleExecutePreparedQuery handles POST /{ledgerName}/prepared-queries/{name}/execute
func (s *Server) handleExecutePreparedQuery(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	queryName := chi.URLParam(r, "queryName")
	if ledgerName == "" || queryName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name and query name are required"))
		return
	}

	var body struct {
		Parameters     map[string]string `json:"parameters"`
		PageSize       uint32            `json:"pageSize"`
		Cursor         string            `json:"cursor"`
		MinLogSequence uint64            `json:"minLogSequence"`
		Mode           string            `json:"mode"`
	}
	if r.Body != nil && r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
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

	req := &servicepb.ExecutePreparedQueryRequest{
		Ledger:         ledgerName,
		QueryName:      queryName,
		Parameters:     body.Parameters,
		PageSize:       body.PageSize,
		Cursor:         body.Cursor,
		MinLogSequence: body.MinLogSequence,
		Mode:           mode,
	}

	resp, err := s.backend.ExecutePreparedQuery(r.Context(), req)
	if err != nil {
		handleError(w, r, err)
		return
	}

	writeJSONResponse(w, http.StatusOK, resp)
}
