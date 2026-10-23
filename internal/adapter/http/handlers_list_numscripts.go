package http

import (
	"net/http"
	"slices"
	"strings"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
)

// handleListNumscripts handles GET /{ledgerName}/numscripts to list the greatest
// version of every numscript for a ledger, paged by name.
func (s *Server) handleListNumscripts(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	page, ok := parsePageQuery(w, r)
	if !ok {
		return
	}

	scripts, err := s.backend.ListNumscripts(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	name := func(n *ledgerpb.NumscriptInfo) string { return n.GetName() }
	slices.SortFunc(scripts, func(a, b *ledgerpb.NumscriptInfo) int { return strings.Compare(name(a), name(b)) })

	scripts, links := pageSorted(page, scripts, name)
	writePageOK(w, r, scripts, links)
}
