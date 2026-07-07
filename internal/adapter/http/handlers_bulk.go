package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleBulk handles POST /{ledgerName}/bulk to create multiple transactions/operations.
func (s *Server) handleBulk(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	// Parse JSON array of bulk elements directly into typed structs.
	var elements []*servicepb.BulkElement

	err := json.UnmarshalRead(r.Body, &elements)
	if err != nil {
		writeBulkErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)

		return
	}

	if s.bulkMaxSize > 0 && len(elements) > s.bulkMaxSize {
		writeBulkErrorResponse(w, http.StatusRequestEntityTooLarge, "BULK_SIZE_EXCEEDED",
			fmt.Errorf("bulk size exceeded, max size is %d", s.bulkMaxSize))

		return
	}

	// Per-element scope check: each element may require a different granular
	// scope. Enforcement keys off auth being enabled — same as RequireScope and
	// the gRPC Apply handler — so an anonymous request with an empty or nil scope
	// set is denied by HasScope rather than skipped.
	if s.authCfg.Enabled {
		effective := internalauth.ExpandedScopesFromContext(r.Context())
		authPresented := internalauth.AuthPresentedFromContext(r.Context())

		for i, elem := range elements {
			req := convertBulkElementToRequest(ledgerName, elem)

			required := internalauth.RequiredScopeForRequest(req)
			if internalauth.HasScope(effective, required) {
				continue
			}

			// 401 when no credentials were presented (the anonymous fallback
			// covers reads only); 403 when a valid token was presented but
			// lacks the required scope.
			if !authPresented {
				writeBulkErrorResponse(w, http.StatusUnauthorized, "UNAUTHENTICATED",
					fmt.Errorf("element %d requires scope %s", i, required))

				return
			}

			writeBulkErrorResponse(w, http.StatusForbidden, "PERMISSION_DENIED",
				fmt.Errorf("element %d requires scope %s", i, required))

			return
		}
	}

	// Process bulk
	opts := bulkOptions{
		continueOnFailure: queryParamBool(r, "continueOnFailure"),
		atomic:            queryParamBool(r, "atomic"),
		idempotencyKey:    r.Header.Get("Idempotency-Key"),
	}
	results := s.runBulk(r.Context(), ledgerName, elements, opts)

	// Write response
	writeBulkResponse(w, elements, results, opts.continueOnFailure)
}

// bulkOptions contains options for bulk processing.
type bulkOptions struct {
	continueOnFailure bool
	atomic            bool
	// idempotencyKey is the batch-level key (Idempotency-Key header) used when
	// atomic — the whole bulk is one proposal, so it has one identity. In
	// non-atomic mode each element keeps its own per-element key instead.
	idempotencyKey string
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
		Action: elem.Action,
	}

	return &servicepb.Request{
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

	// Build requests slice + parallel per-element idempotency keys (used only in
	// non-atomic mode, where each element is its own proposal).
	requests := make([]*servicepb.Request, len(elements))
	keys := make([]string, len(elements))
	for i, elem := range elements {
		requests[i] = convertBulkElementToRequest(ledgerName, elem)
		keys[i] = elem.IdempotencyKey
	}

	if opts.atomic {
		return s.runBulkAtomic(ctx, opts.idempotencyKey, requests)
	}

	return s.runBulkSequential(ctx, requests, keys, opts.continueOnFailure)
}

// runBulkAtomic applies all requests as one atomic batch under a single
// idempotency key (the bulk-level Idempotency-Key header).
func (s *Server) runBulkAtomic(ctx context.Context, idempotencyKey string, requests []*servicepb.Request) []bulkResult {
	results := make([]bulkResult, len(requests))

	logs, err := s.applyUnsigned(ctx, idempotencyKey, requests...)
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

// runBulkSequential applies requests one by one, each as its own proposal under
// its per-element idempotency key.
func (s *Server) runBulkSequential(ctx context.Context, requests []*servicepb.Request, keys []string, continueOnFailure bool) []bulkResult {
	results := make([]bulkResult, len(requests))
	hasError := false

	for i, request := range requests {
		if hasError && !continueOnFailure {
			results[i] = bulkResult{err: context.Canceled}

			continue
		}

		logs, err := s.applyUnsigned(ctx, keys[i], request)
		if err != nil {
			hasError = true
			results[i] = bulkResult{err: err}

			continue
		}

		if len(logs) == 0 {
			hasError = true
			results[i] = bulkResult{err: errors.New("no log returned from apply")}

			continue
		}

		results[i] = bulkResult{log: logs[0].GetPayload().GetApply().GetLog()}
	}

	return results
}

// writeBulkResponse writes the bulk response. Per-element errors are bucketed
// into four classes that decide the top-level status; the ordering mirrors
// handleError so a given error surfaces the same HTTP status inside a bulk as
// it would in a single request:
//
//  1. **Retryable infra** — `commonpb.ErrNoLeader`, `plan.ErrCacheHorizonExceeded`,
//     or any `domain.Describable` whose Kind is Unavailable. Top-level `503`
//     with `Retry-After: 1`, regardless of `continueOnFailure` (mirrors
//     handleError's retryable-sentinel branch).
//  2. **Non-retryable infra** — anything else that is not a domain business
//     outcome, or a Describable whose Kind is Internal/ResourceExhausted.
//     Top-level `500`, regardless of `continueOnFailure`.
//  3. **Aborted** (`context.Canceled` sentinel emitted by runBulkSequential
//     when `continueOnFailure=false` and a prior element failed): these
//     elements were never attempted and contribute nothing to the top-level
//     status — the original failing element drives the rollup.
//  4. **Per-element business failures** — a Describable whose Kind maps to a
//     4xx status. Rolled up per `continueOnFailure`: `200` when opt-in, `400`
//     otherwise.
func writeBulkResponse(w http.ResponseWriter, elements []*servicepb.BulkElement, results []bulkResult, continueOnFailure bool) {
	hasDomainError := false
	hasRetryableInfraError := false
	hasFatalInfraError := false
	apiResults := make([]bulkAPIResult, len(results))

	for i, result := range results {
		responseType := servicepb.GetLedgerActionType(elements[i].Action)

		var data any

		if result.err != nil {
			switch classifyBulkError(result.err) {
			case bulkErrClassAborted:
				// Skipped element — no top-level status contribution.
			case bulkErrClassDomain:
				hasDomainError = true
			case bulkErrClassRetryableInfra:
				hasRetryableInfraError = true
			case bulkErrClassFatalInfra:
				hasFatalInfraError = true
			}

			apiResults[i] = bulkAPIResult{
				ResponseType:     "ERROR",
				ErrorCode:        bulkErrorCode(result.err),
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

	statusCode := http.StatusOK
	switch {
	case hasFatalInfraError:
		statusCode = http.StatusInternalServerError
	case hasRetryableInfraError:
		// Match handleError's leader-loss / cache-horizon branch: the caller
		// must retry, and Retry-After gives them the same backoff hint they
		// get on single requests.
		w.Header().Set("Retry-After", "1")
		statusCode = http.StatusServiceUnavailable
	case hasDomainError && !continueOnFailure:
		statusCode = http.StatusBadRequest
	}

	response := bulkResponse{Data: apiResults}
	writeJSONResponse(w, statusCode, response)
}

// bulkErrClass tags where a per-element error counts in the top-level status
// roll-up.
type bulkErrClass int

const (
	bulkErrClassAborted bulkErrClass = iota
	bulkErrClassDomain
	bulkErrClassRetryableInfra
	bulkErrClassFatalInfra
)

// classifyBulkError decides which bucket a per-element error falls into. The
// checks mirror the ones in handleError so bulk stays consistent with single
// requests: the same error would produce the same HTTP status outside a bulk.
func classifyBulkError(err error) bulkErrClass {
	if errors.Is(err, context.Canceled) {
		return bulkErrClassAborted
	}

	// Retryable infra sentinels handled before Describable dispatch, mirroring
	// handleError.
	if errors.Is(err, commonpb.ErrNoLeader) || errors.Is(err, plan.ErrCacheHorizonExceeded) {
		return bulkErrClassRetryableInfra
	}

	var d domain.Describable
	if errors.As(err, &d) {
		// A Describable's Kind decides whether the outcome is a business
		// failure (4xx → per-element), a retryable infra condition
		// (Unavailable → 503), or a fatal infra condition (Internal /
		// ResourceExhausted → 500).
		switch domain.Kind(d) {
		case domain.KindUnavailable:
			return bulkErrClassRetryableInfra
		case domain.KindInternal, domain.KindResourceExhausted:
			return bulkErrClassFatalInfra
		default:
			return bulkErrClassDomain
		}
	}

	// Unknown error: assume fatal infra so it can't be masked as a 200.
	return bulkErrClassFatalInfra
}

// bulkErrorCode returns a machine-readable code for a per-element bulk failure.
// Domain-typed errors expose it through the Describable contract; anything else
// keeps the generic "ERROR" fallback rather than leaking a raw string.
func bulkErrorCode(err error) string {
	var d domain.Describable
	if errors.As(err, &d) {
		return d.Reason()
	}

	return "ERROR"
}

// writeBulkErrorResponse writes a bulk error response.
func writeBulkErrorResponse(w http.ResponseWriter, statusCode int, errorCode string, err error) {
	errorMsg := ""
	if err != nil {
		errorMsg = err.Error()
	}

	writeJSONResponse(w, statusCode, bulkResponse{
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
