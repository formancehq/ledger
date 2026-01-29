package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/go-chi/chi/v5"
)

// handleBulk handles POST /{ledgerName}/_bulk to create multiple transactions/operations
func (s *Server) handleBulk(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	ledgerInfo, err := s.backend.GetLedgerByName(r.Context(), ledgerName)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)
		return
	}

	// Parse JSON array of bulk elements
	var rawElements []json.RawValue
	if err := json.UnmarshalRead(r.Body, &rawElements); err != nil {
		writeBulkErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)
		return
	}

	if s.bulkMaxSize > 0 && len(rawElements) > s.bulkMaxSize {
		writeBulkErrorResponse(w, http.StatusRequestEntityTooLarge, "BULK_SIZE_EXCEEDED",
			fmt.Errorf("bulk size exceeded, max size is %d", s.bulkMaxSize))
		return
	}

	// Parse each element
	elements := make([]*servicepb.LedgerApplyAction, 0, len(rawElements))
	for i, rawElem := range rawElements {
		elem := &servicepb.LedgerApplyAction{}
		if err := json.Unmarshal(rawElem, elem); err != nil {
			writeBulkErrorResponse(w, http.StatusBadRequest, "VALIDATION",
				fmt.Errorf("error parsing element %d: %w", i, err))
			return
		}
		elements = append(elements, elem)
	}

	// Process bulk
	opts := bulkOptions{
		continueOnFailure: queryParamBool(r, "continueOnFailure"),
		atomic:            queryParamBool(r, "atomic"),
	}
	results := s.runBulk(r.Context(), ledgerInfo.Id, elements, opts)

	// Write response
	writeBulkResponse(w, elements, results)
}

// bulkOptions contains options for bulk processing
type bulkOptions struct {
	continueOnFailure bool
	atomic            bool
}

// bulkResult represents the result of a single bulk element
type bulkResult struct {
	log *commonpb.LedgerLog
	err error
}

// runBulk processes a list of bulk elements and returns the results
func (s *Server) runBulk(ctx context.Context, ledgerID uint32, elements []*servicepb.LedgerApplyAction, opts bulkOptions) []bulkResult {
	if len(elements) == 0 {
		return nil
	}

	// Build actions slice
	actions := make([]*servicepb.Action, len(elements))
	for i, elem := range elements {
		elem.LedgerId = ledgerID
		actions[i] = &servicepb.Action{
			Type: &servicepb.Action_Apply{
				Apply: elem,
			},
		}
	}

	if opts.atomic {
		return s.runBulkAtomic(ctx, actions)
	}
	return s.runBulkSequential(ctx, actions, opts.continueOnFailure)
}

// runBulkAtomic applies all actions in a single batch
func (s *Server) runBulkAtomic(ctx context.Context, actions []*servicepb.Action) []bulkResult {
	results := make([]bulkResult, len(actions))

	logs, err := s.backend.Apply(ctx, actions...)
	if err != nil {
		// In atomic mode, if any action fails, all fail with the same error
		for i := range results {
			results[i] = bulkResult{err: err}
		}
		return results
	}

	for i, log := range logs {
		results[i] = bulkResult{log: log.GetApply().GetLog()}
	}
	return results
}

// runBulkSequential applies actions one by one
func (s *Server) runBulkSequential(ctx context.Context, actions []*servicepb.Action, continueOnFailure bool) []bulkResult {
	results := make([]bulkResult, len(actions))
	hasError := false

	for i, action := range actions {
		if hasError && !continueOnFailure {
			results[i] = bulkResult{err: context.Canceled}
			continue
		}

		logs, err := s.backend.Apply(ctx, action)
		if err != nil {
			hasError = true
			results[i] = bulkResult{err: err}
			continue
		}

		results[i] = bulkResult{log: logs[0].GetApply().GetLog()}
	}

	return results
}

// writeBulkResponse writes the bulk response
func writeBulkResponse(w http.ResponseWriter, elements []*servicepb.LedgerApplyAction, results []bulkResult) {
	hasError := false
	apiResults := make([]bulkAPIResult, len(results))

	for i, result := range results {
		responseType := servicepb.GetLedgerApplyActionType(elements[i])
		var data any

		if result.err != nil {
			hasError = true
			apiResults[i] = bulkAPIResult{
				ResponseType:     "ERROR",
				ErrorCode:        "ERROR",
				ErrorDescription: result.err.Error(),
			}
			continue
		}

		// Extract data from log
		if log := result.log; log != nil {
			apiResults[i].LogID = log.Id
			if log.Data != nil {
				if ct := log.Data.GetCreatedTransaction(); ct != nil {
					data = ct.Transaction
				}
			}
		}

		apiResults[i] = bulkAPIResult{
			ResponseType: responseType,
			Data:         data,
			LogID:        apiResults[i].LogID,
		}
	}

	if hasError {
		w.WriteHeader(http.StatusBadRequest)
	}

	response := bulkResponse{Data: apiResults}

	w.Header().Set("Content-Type", "application/json")
	if err := json.MarshalWrite(w, response); err != nil {
		panic(err)
	}
}

// writeBulkErrorResponse writes a bulk error response
func writeBulkErrorResponse(w http.ResponseWriter, statusCode int, errorCode string, err error) {
	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.MarshalWrite(w, bulkResponse{
		ErrorCode:    errorCode,
		ErrorMessage: errorMsg,
	})
}

// bulkAPIResult represents a single result in the API response
type bulkAPIResult struct {
	ErrorCode        string `json:"errorCode,omitempty"`
	ErrorDescription string `json:"errorDescription,omitempty"`
	Data             any    `json:"data,omitempty"`
	ResponseType     string `json:"responseType"`
	LogID            uint64 `json:"logID"`
}

// bulkResponse is the response structure for bulk operations
type bulkResponse struct {
	Data         []bulkAPIResult `json:"data,omitempty"`
	ErrorCode    string          `json:"errorCode,omitempty"`
	ErrorMessage string          `json:"errorMessage,omitempty"`
}
