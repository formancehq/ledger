package http

import (
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
)

// handleListAccounts handles GET /{ledgerName}/accounts to list accounts.
func (s *Server) handleListAccounts(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	// Parse query parameters
	pageSize, ok := parsePageSize(w, r)
	if !ok {
		return
	}

	afterAddress := r.URL.Query().Get("after")
	prefix := r.URL.Query().Get("prefix")

	// Build an optional address-prefix filter from query parameter
	var filter *commonpb.QueryFilter
	if prefix != "" {
		filter = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix},
				},
			},
		}
	}

	reverse := r.URL.Query().Get("reverse") == "true"

	ctx, profile := query.WithProfile(r.Context())

	cursor, err := s.backend.ListAccounts(ctx, ledgerName, pageSize, afterAddress, filter, reverse)
	if err != nil {
		handleError(w, r, err)

		return
	}

	accounts, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	if wantsHTTPProfile(r) {
		writeProfileHeader(w, profile)
	}

	writeOK(w, accounts)
}
