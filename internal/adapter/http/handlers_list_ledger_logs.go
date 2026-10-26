package http

import (
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// handleListLedgerLogs handles GET /{ledgerName}/logs to list logs for a
// specific ledger, paged by ledger-local log id, with optional date ranges
// (startDate/endDate).
func (s *Server) handleListLedgerLogs(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	page, ok := parsePageQuery(w, r)
	if !ok {
		return
	}

	afterLogID, err := query.CursorUint64(page.cursor)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	var filters []*commonpb.QueryFilter

	// Build date range filter from startDate/endDate query parameters (RFC3339).
	dateCond := &commonpb.UintCondition{}
	hasDateFilter := false

	if sd := r.URL.Query().Get("startDate"); sd != "" {
		v, ok := parseFilterDateMicros(w, "startDate", sd)
		if !ok {
			return
		}

		dateCond.Min = &v
		hasDateFilter = true
	}

	if ed := r.URL.Query().Get("endDate"); ed != "" {
		v, ok := parseFilterDateMicros(w, "endDate", ed)
		if !ok {
			return
		}

		dateCond.Max = &v
		dateCond.MaxExclusive = true
		hasDateFilter = true
	}

	if hasDateFilter {
		filters = append(filters, &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogBuiltinUint{
				LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
					Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
					Cond:  dateCond,
				},
			},
		})
	}

	// The generic `filter` query parameter accepts either the textual filterexpr
	// grammar or the structured v2 JSON DSL (EN-1511); it is AND-combined with the
	// startDate/endDate convenience params above.
	generic, ok := parseListFilter(w, r, commonpb.QueryTarget_QUERY_TARGET_LOGS)
	if !ok {
		return
	}

	filters = append(filters, generic)

	filter := combineFilters(filters...)

	cursor, err := s.backend.ListLogs(r.Context(), ledgerName, afterLogID, page.fetchSize(), filter, page.reverse)
	if err != nil {
		handleError(w, r, err)

		return
	}

	logs, links, ok := drainPage(w, r, page, cursor, func(l *commonpb.Log) string {
		apply := l.GetPayload().GetApply()
		if apply == nil {
			return ""
		}

		return strconv.FormatUint(apply.GetLog().GetId(), 10)
	})
	if !ok {
		return
	}

	writePageOK(w, r, logs, links)
}
