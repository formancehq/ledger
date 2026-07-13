package http

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// accountTypeBody is the camelCase JSON representation of an account type,
// shared by the add-account-type and create-ledger handlers so both endpoints
// accept the full model gRPC exposes.
type accountTypeBody struct {
	Name    string `json:"name"`
	Pattern string `json:"pattern"`
	// Persistence controls how account volumes are stored: NORMAL (default),
	// EPHEMERAL, or TRANSIENT. Empty defaults to NORMAL.
	Persistence string `json:"persistence,omitempty"`
	// SegmentTypes maps a pattern variable name to a type constraint
	// (regex/uuid/uint64/bytes).
	SegmentTypes map[string]*commonpb.SegmentTypeJSON `json:"segmentTypes,omitempty"`
}

// toProto converts the HTTP request body to a proto AccountType, reusing the
// shared commonpb conversion helpers.
func (b accountTypeBody) toProto() (*commonpb.AccountType, error) {
	persistence, err := commonpb.ParsePersistence(b.Persistence)
	if err != nil {
		return nil, err
	}

	at := &commonpb.AccountType{
		Name:        b.Name,
		Pattern:     b.Pattern,
		Persistence: persistence,
	}

	if len(b.SegmentTypes) > 0 {
		at.SegmentTypes = make(map[string]*commonpb.SegmentType, len(b.SegmentTypes))
		for name, st := range b.SegmentTypes {
			converted, err := commonpb.SegmentTypeFromJSON(st)
			if err != nil {
				return nil, fmt.Errorf("segmentTypes[%q]: %w", name, err)
			}

			at.SegmentTypes[name] = converted
		}
	}

	return at, nil
}

// handleAddAccountType handles POST /{ledgerName}/account-types.
func (s *Server) handleAddAccountType(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	var body accountTypeBody
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

	accountType, err := body.toProto()
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	_, err = s.applyUnsigned(r.Context(), "", &servicepb.Request{
		Type: &servicepb.Request_AddAccountType{
			AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
				Ledger:      ledgerName,
				AccountType: accountType,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.WriteHeader(http.StatusCreated)
}
