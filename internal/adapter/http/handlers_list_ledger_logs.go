package http

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// handleListLedgerLogs handles GET /{ledgerName}/logs to list logs for a specific ledger.
// It builds a LedgerCondition filter from the URL path and delegates to ListLogs.
func (s *Server) handleListLedgerLogs(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	pageSize, ok := parsePageSize(w, r)
	if !ok {
		return
	}

	filters := []*commonpb.QueryFilter{
		{
			Filter: &commonpb.QueryFilter_Ledger{
				Ledger: &commonpb.LedgerCondition{
					Cond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ledgerName},
					},
				},
			},
		},
	}

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
		t, err := time.Parse(time.RFC3339, sd)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid startDate parameter, expected RFC3339 format"))

			return
		}

		v := uint64(t.UnixMicro())
		dateCond.Min = &v
		hasDateFilter = true
	}

	if ed := r.URL.Query().Get("endDate"); ed != "" {
		t, err := time.Parse(time.RFC3339, ed)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid endDate parameter, expected RFC3339 format"))

			return
		}

		v := uint64(t.UnixMicro())
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

	filter := filters[0]
	if len(filters) > 1 {
		filter = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{Filters: filters},
			},
		}
	}

	cursor, err := s.backend.ListLogs(r.Context(), 0, pageSize, filter)
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
