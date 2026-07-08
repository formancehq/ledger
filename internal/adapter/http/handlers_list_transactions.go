package http

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// handleListTransactions handles GET /{ledgerName}/transactions to list a
// ledger's transactions. Query params:
//
//   - pageSize
//   - after=<txID>       cursor (exclusive)
//   - reverse=true       reverse the default order — see convention below
//   - startDate/endDate  RFC3339, filter on transaction timestamp.
//     Requires the builtin `TX_BUILTIN_INDEX_TIMESTAMP` index to be
//     enabled on the ledger via `CreateIndex`.
//   - reference          exact-match reference filter
//
// Ordering convention (mirrors `ctrl.Controller.ListTransactions`): the
// default (reverse=false) returns newest-first (descending transaction id);
// reverse=true returns oldest-first (ascending).
func (s *Server) handleListTransactions(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	pageSize, ok := parsePageSize(w, r)
	if !ok {
		return
	}

	var afterTxID uint64

	if after := r.URL.Query().Get("after"); after != "" {
		parsed, err := strconv.ParseUint(after, 10, 64)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid after parameter"))

			return
		}

		afterTxID = parsed
	}

	reverse := r.URL.Query().Get("reverse") == "true"

	var filters []*commonpb.QueryFilter

	if ref := r.URL.Query().Get("reference"); ref != "" {
		filters = append(filters, &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Reference{
				Reference: &commonpb.ReferenceCondition{
					Cond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ref},
					},
				},
			},
		})
	}

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
			Filter: &commonpb.QueryFilter_BuiltinUint{
				BuiltinUint: &commonpb.BuiltinUintCondition{
					Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
					Cond:  dateCond,
				},
			},
		})
	}

	var filter *commonpb.QueryFilter
	if len(filters) == 1 {
		filter = filters[0]
	} else if len(filters) > 1 {
		filter = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{Filters: filters},
			},
		}
	}

	ctx, profile := query.WithProfile(r.Context())

	cursor, err := s.backend.ListTransactions(ctx, ledgerName, pageSize, afterTxID, filter, reverse)
	if err != nil {
		handleError(w, r, err)

		return
	}

	transactions, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	if wantsHTTPProfile(r) {
		writeProfileHeader(w, profile)
	}

	writeOK(w, transactions)
}
