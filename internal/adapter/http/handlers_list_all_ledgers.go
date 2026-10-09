package http

import (
	"net/http"
	"slices"
	"strings"

	"github.com/formancehq/ledger/v3/internal/pkg/sensitive"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// handleListAllLedgers handles GET / to list ledgers, paged by name.
func (s *Server) handleListAllLedgers(w http.ResponseWriter, r *http.Request) {
	page, ok := parsePageQuery(w, r)
	if !ok {
		return
	}

	cursor, err := s.backend.ListLedgers(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	ledgers, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	name := func(l *commonpb.LedgerInfo) string { return l.GetName() }
	slices.SortFunc(ledgers, func(a, b *commonpb.LedgerInfo) int { return strings.Compare(name(a), name(b)) })

	ledgers, links := pageSorted(page, ledgers, name)
	for i, ledger := range ledgers {
		ledgers[i] = sensitive.Redact(ledger)
	}
	writePageOK(w, r, ledgers, links)
}
