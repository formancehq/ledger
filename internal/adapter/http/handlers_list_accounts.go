package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
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

	// The `filter` query parameter accepts either the textual filterexpr grammar
	// or the structured v2 JSON DSL (EN-1511). An address-prefix selection is
	// expressed through it as the textual `address ^= "<prefix>"` (or structured
	// `{"$match":{"address":"<prefix>:"}}`); there is no separate `prefix` alias.
	filter, ok := parseListFilter(w, r, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	if !ok {
		return
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
