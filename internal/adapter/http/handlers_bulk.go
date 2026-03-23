package http

import (
	"context"
	"fmt"
	"net/http"

	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
	"github.com/formancehq/ledger-v3-poc/internal/adapter/json"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// handleBulk handles POST /{ledgerName}/_bulk to create multiple transactions/operations.
func (s *Server) handleBulk(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	// Parse JSON array of bulk elements
	var rawElements []json.RawValue

	err := json.UnmarshalRead(r.Body, &rawElements)
	if err != nil {
		writeBulkErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)

		return
	}

	if s.bulkMaxSize > 0 && len(rawElements) > s.bulkMaxSize {
		writeBulkErrorResponse(w, http.StatusRequestEntityTooLarge, "BULK_SIZE_EXCEEDED",
			fmt.Errorf("bulk size exceeded, max size is %d", s.bulkMaxSize))

		return
	}

	// Parse each element
	elements := make([]*servicepb.BulkElement, 0, len(rawElements))
	for i, rawElem := range rawElements {
		elem := &servicepb.BulkElement{}

		err := json.Unmarshal(rawElem, elem)
		if err != nil {
			writeBulkErrorResponse(w, http.StatusBadRequest, "VALIDATION",
				fmt.Errorf("error parsing element %d: %w", i, err))

			return
		}

		elements = append(elements, elem)
	}

	// Per-element scope check: verify the caller has the required scope for each element.
	effective := internalauth.ExpandedScopesFromContext(r.Context())
	if effective != nil {
		for i, elem := range elements {
			req := convertBulkElementToRequest(ledgerName, elem)

			required := internalauth.RequiredScopeForRequest(req)
			if !internalauth.HasScope(effective, required) {
				writeBulkErrorResponse(w, http.StatusForbidden, "PERMISSION_DENIED",
					fmt.Errorf("element %d requires scope %s", i, required))

				return
			}
		}
	}

	// Process bulk
	opts := bulkOptions{
		continueOnFailure: queryParamBool(r, "continueOnFailure"),
		atomic:            queryParamBool(r, "atomic"),
	}
	results := s.runBulk(r.Context(), ledgerName, elements, opts)

	// Write response
	writeBulkResponse(w, elements, results)
}

// bulkOptions contains options for bulk processing.
type bulkOptions struct {
	continueOnFailure bool
	atomic            bool
}

// bulkResult represents the result of a single bulk element.
type bulkResult struct {
	log *commonpb.LedgerLog
	err error
}

// convertBulkElementToRequest converts a servicepb.BulkElement to a servicepb.Request.
func convertBulkElementToRequest(ledgerName string, elem *servicepb.BulkElement) *servicepb.Request {
	applyRequest := &servicepb.LedgerApplyRequest{
		Ledger: ledgerName,
		Data:   elem.Action.GetData(),
	}

	return &servicepb.Request{
		IdempotencyKey: elem.IdempotencyKey,
		Type: &servicepb.Request_Apply{
			Apply: applyRequest,
		},
	}
}

// runBulk processes a list of bulk elements and returns the results.
func (s *Server) runBulk(ctx context.Context, ledgerName string, elements []*servicepb.BulkElement, opts bulkOptions) []bulkResult {
	if len(elements) == 0 {
		return nil
	}

	// Build requests slice
	requests := make([]*servicepb.Request, len(elements))
	for i, elem := range elements {
		requests[i] = convertBulkElementToRequest(ledgerName, elem)
	}

	if opts.atomic {
		return s.runBulkAtomic(ctx, requests)
	}

	return s.runBulkSequential(ctx, requests, opts.continueOnFailure)
}

// runBulkAtomic applies all requests in a single batch.
func (s *Server) runBulkAtomic(ctx context.Context, requests []*servicepb.Request) []bulkResult {
	results := make([]bulkResult, len(requests))

	logs, err := s.backend.Apply(ctx, requests...)
	if err != nil {
		// In atomic mode, if any action fails, all fail with the same error
		for i := range results {
			results[i] = bulkResult{err: err}
		}

		return results
	}

	for i, log := range logs {
		results[i] = bulkResult{log: log.GetPayload().GetApply().GetLog()}
	}

	return results
}

// runBulkSequential applies requests one by one.
func (s *Server) runBulkSequential(ctx context.Context, requests []*servicepb.Request, continueOnFailure bool) []bulkResult {
	results := make([]bulkResult, len(requests))
	hasError := false

	for i, request := range requests {
		if hasError && !continueOnFailure {
			results[i] = bulkResult{err: context.Canceled}

			continue
		}

		logs, err := s.backend.Apply(ctx, request)
		if err != nil {
			hasError = true
			results[i] = bulkResult{err: err}

			continue
		}

		results[i] = bulkResult{log: logs[0].GetPayload().GetApply().GetLog()}
	}

	return results
}

// writeBulkResponse writes the bulk response.
func writeBulkResponse(w http.ResponseWriter, elements []*servicepb.BulkElement, results []bulkResult) {
	hasError := false
	apiResults := make([]bulkAPIResult, len(results))

	for i, result := range results {
		responseType := servicepb.GetLedgerApplyActionType(elements[i].Action)

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
			apiResults[i].LogID = log.GetId()
			if log.GetData() != nil {
				if ct := log.GetData().GetCreatedTransaction(); ct != nil {
					data = ct.GetTransaction()
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

	err := json.MarshalWrite(w, response)
	if err != nil {
		panic(err)
	}
}

// writeBulkErrorResponse writes a bulk error response.
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

// bulkAPIResult represents a single result in the API response.
type bulkAPIResult struct {
	ErrorCode        string `json:"errorCode,omitempty"`
	ErrorDescription string `json:"errorDescription,omitempty"`
	Data             any    `json:"data,omitempty"`
	ResponseType     string `json:"responseType"`
	LogID            uint64 `json:"logID"`
}

// bulkResponse is the response structure for bulk operations.
type bulkResponse struct {
	Data         []bulkAPIResult `json:"data,omitempty"`
	ErrorCode    string          `json:"errorCode,omitempty"`
	ErrorMessage string          `json:"errorMessage,omitempty"`
}
