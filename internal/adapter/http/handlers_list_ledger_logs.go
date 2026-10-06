package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// handleListLedgerLogs handles GET /{ledgerName}/logs to list logs for a specific ledger.
// It passes the ledger name directly to ListLogs and builds optional filters
// for pagination (after) and date ranges (startDate/endDate).
func (s *Server) handleListLedgerLogs(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	pageSize, ok := parsePageSize(w, r)
	if !ok {
		return
	}

	var filters []*commonpb.QueryFilter

	if after := r.URL.Query().Get("after"); after != "" {
		parsed, err := strconv.ParseUint(after, 10, 64)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid after parameter"))

			return
		}

		filters = append(filters, &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogId{
				LogId: &commonpb.LogIdCondition{
					Cond: &commonpb.UintCondition{
						Min:          &parsed,
						MinExclusive: true,
					},
				},
			},
		})
	}

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
	// after/startDate/endDate convenience params above.
	generic, ok := parseListFilter(w, r, commonpb.QueryTarget_QUERY_TARGET_LOGS)
	if !ok {
		return
	}

	filters = append(filters, generic)

	filter := combineFilters(filters...)

	cursor, err := s.backend.ListLogs(r.Context(), ledgerName, 0, pageSize, filter)
	if err != nil {
		handleError(w, r, err)

		return
	}

	logs, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	writeOK(w, logs)
}
