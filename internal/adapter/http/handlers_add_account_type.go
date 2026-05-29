package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type addAccountTypeRequest struct {
	Name    string `json:"name"`
	Pattern string `json:"pattern"`
}

// handleAddAccountType handles POST /{ledgerName}/account-types.
func (s *Server) handleAddAccountType(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	var body addAccountTypeRequest
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid request body: %w", err))

		return
	}

	if body.Name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("name is required"))

		return
	}

	if body.Pattern == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("pattern is required"))

		return
	}

	_, err := s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_AddAccountType{
			AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
				Ledger: ledgerName,
				AccountType: &commonpb.AccountType{
					Name:    body.Name,
					Pattern: body.Pattern,
				},
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusCreated)
}
