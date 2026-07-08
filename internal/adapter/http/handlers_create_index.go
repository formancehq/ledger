package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// createIndexBody is the JSON body accepted on POST /{ledgerName}/indexes.
// The id field carries the target IndexID in canonical form
// (e.g. "metadata:TARGET_TYPE_ACCOUNT:color", "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP").
type createIndexBody struct {
	ID string `json:"id"`
}

// handleCreateIndex handles POST /{ledgerName}/indexes to create a new
// index on a ledger. The request body carries the canonical form of the
// target IndexID; the FSM starts the backfill on the next apply.
func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	var body createIndexBody
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	if body.ID == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("id is required"))

		return
	}

	id, err := indexes.ParseCanonical(body.ID)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	if _, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
		Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: ledgerName,
				Id:     id,
			},
		},
	}); err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusNoContent)
}
