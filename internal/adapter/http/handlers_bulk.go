package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime/pprof"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleBulk handles POST /{ledgerName}/bulk to create multiple transactions/operations.
//
// The handler body runs under `pprof.Do` with the `component=admission.http`
// label so Pyroscope (CPU / block/delay / mutex profiles) can attribute its
// cost separately from the FSM pipeline (applier.main, applier.decoder,
// applier.committer). The label propagates to any child goroutines spawned
// inside the handler and — unlike a bare SetGoroutineLabels — is scoped to this
// call, so it does not leak onto subsequent requests served by the same
// goroutine on a reused (HTTP/1 keep-alive) connection.
//
// The labeled callback context is threaded into serveBulk via r.WithContext so
// downstream work reached through r.Context() (runBulk → applyUnsigned → the
// admission/apply path, and any goroutines it spawns) inherits the same pprof
// label rather than only the synchronous handler goroutine carrying it.
func (s *Server) handleBulk(w http.ResponseWriter, r *http.Request) {
	pprof.Do(r.Context(), pprof.Labels("component", "admission.http"), func(ctx context.Context) {
		s.serveBulk(w, r.WithContext(ctx))
	})
}

// serveBulk holds the actual bulk-handling logic, invoked under the
// admission.http pprof label by handleBulk.
func (s *Server) serveBulk(w http.ResponseWriter, r *http.Request) {
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

// writeBulkResponse writes the bulk response. Each per-element error is
// mapped to the HTTP status it would carry as a single request (via
// perElementStatus, which mirrors handleError). The top-level status is
// then the "worst" of those statuses, with two rollup rules:
//
//   - **Aborted** (`context.Canceled` sentinel emitted by runBulkSequential
//     when `continueOnFailure=false` and a prior element failed): perElementStatus
//     returns 0 and the element contributes nothing to the rollup.
//   - **Per-element business** (4xx from a domain Describable): rolled up per
//     `continueOnFailure` — suppressed to 200 when opt-in, surfaced as its
//     own status (typically 400/404/409) otherwise.
//
// Any 5xx/429 status from infra/retryable/rate-limit errors always surfaces,
// with `Retry-After: 1` on 503 to match handleError.
func writeBulkResponse(w http.ResponseWriter, elements []*servicepb.BulkElement, results []bulkResult, continueOnFailure bool) {
	worstBusiness := 0 // highest 4xx from a per-element business failure
	worstInfra := 0    // highest ≥429 from a retryable/infra/rate-limit error
	apiResults := make([]bulkAPIResult, len(results))

	for i, result := range results {
		responseType := servicepb.GetLedgerActionType(elements[i].Action)

		var data any

		if result.err != nil {
			switch st := perElementStatus(result.err); {
			case st == 0:
				// Aborted sentinel — no top-level status contribution.
			case st >= 500:
				if st > worstInfra {
					worstInfra = st
				}
			case st == http.StatusTooManyRequests:
				// KindResourceExhausted: retryable resource-limit, still
				// forced past the continueOnFailure rollup.
				if st > worstInfra {
					worstInfra = st
				}
			default:
				// 4xx per-element business error — subject to continueOnFailure.
				if st > worstBusiness {
					worstBusiness = st
				}
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
	case worstInfra > 0:
		statusCode = worstInfra
		if worstInfra == http.StatusServiceUnavailable {
			// Match handleError's leader-loss / cache-horizon branch: give
			// callers the same backoff hint on single requests.
			w.Header().Set("Retry-After", "1")
		}
	case worstBusiness > 0 && !continueOnFailure:
		statusCode = worstBusiness
	}

	response := bulkResponse{Data: apiResults}
	writeJSONResponse(w, statusCode, response)
}

// perElementStatus returns the HTTP status a given per-element error would
// carry as a single request, or 0 for the runBulkSequential context.Canceled
// sentinel that marks skipped elements. The mapping mirrors handleError so
// bulk stays consistent with single-request handling.
func perElementStatus(err error) int {
	// Aborted sentinel: runBulkSequential sets this on remaining elements
	// after the first failure when continueOnFailure=false. Not a status
	// contributor — the originating error drives the rollup.
	if errors.Is(err, context.Canceled) {
		return 0
	}

	// Retryable infra sentinels handled before Describable dispatch,
	// mirroring handleError.
	if errors.Is(err, commonpb.ErrNoLeader) || errors.Is(err, plan.ErrCacheHorizonExceeded) {
		return http.StatusServiceUnavailable
	}

	var d domain.Describable
	if errors.As(err, &d) {
		return kindToHTTPStatus(domain.Kind(d))
	}

	// Unknown error: 500. Can't be masked as 200 under continueOnFailure.
	return http.StatusInternalServerError
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
