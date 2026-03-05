package http

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/go-chi/chi/v5"
)

// handleListLedgerLogs handles GET /{ledgerName}/logs to list logs for a specific ledger.
// It builds a LedgerCondition filter from the URL path and delegates to ListLogs.
func (s *Server) handleListLedgerLogs(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))
		return
	}

	var pageSize uint32
	if ps := r.URL.Query().Get("pageSize"); ps != "" {
		parsed, err := strconv.ParseUint(ps, 10, 32)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid pageSize parameter"))
			return
		}
		pageSize = uint32(parsed)
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
	defer func() {
		_ = cursor.Close()
	}()

	var logs []*commonpb.Log
	for {
		log, err := cursor.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			handleError(w, r, err)
			return
		}
		logs = append(logs, log)
	}

	writeOK(w, logs)
}
