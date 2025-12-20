package http

import (
	"errors"
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger-v3-poc/internal/http/bulking"
	"github.com/go-chi/chi/v5"
)

// handleBulk handles POST /{ledgerName}/_bulk to create multiple transactions/operations
func (s *Server) handleBulk(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		api.WriteErrorResponse(w, http.StatusBadRequest, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	ledgerCluster, err := s.cluster.GetLedgerCluster(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)
		return
	}

	// Determine content type
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	if strings.Index(contentType, ";") > 0 {
		contentType = strings.Split(contentType, ";")[0]
	}

	// Get handler factory for content type
	bulkHandlerFactory, ok := s.bulkHandlerFactories[contentType]
	if !ok {
		api.WriteErrorResponse(w, http.StatusBadRequest, "VALIDATION", errors.New("unsupported content type: "+contentType))
		return
	}

	bulkHandler := bulkHandlerFactory.CreateBulkHandler()
	send, receive, ok := bulkHandler.GetChannels(w, r)
	if !ok {
		return
	}

	// Create bulker and run
	err = s.bulkerFactory.CreateBulker(ledgerCluster, ledgerName).Run(r.Context(), send, receive,
		bulking.BulkingOptions{
			ContinueOnFailure: api.QueryParamBool(r, "continueOnFailure"),
			Atomic:            api.QueryParamBool(r, "atomic"),
			Parallel:          api.QueryParamBool(r, "parallel"),
		},
	)
	if err != nil {
		switch {
		case errors.Is(err, bulking.ErrAtomicParallelConflict):
			api.WriteErrorResponse(w, http.StatusPreconditionFailed, "VALIDATION", err)
		default:
			handleError(w, r, err)
		}
		return
	}

	bulkHandler.Terminate(w, r)
}
