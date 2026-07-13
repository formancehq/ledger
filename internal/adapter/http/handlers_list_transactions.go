package http

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// parseFilterDateMicros parses an RFC3339 date query parameter and returns it
// as Unix microseconds for a UintCondition bound. Transaction timestamps are
// stored as unsigned microseconds, so a pre-epoch (negative UnixMicro) date
// has no representable bound: casting it to uint64 would wrap to a huge value
// and silently corrupt the filter (a start bound would exclude everything, an
// end bound would include everything). Such dates are rejected with 400 rather
// than accepted with garbage semantics. On error it writes the response and
// returns ok=false; the caller must return immediately.
func parseFilterDateMicros(w http.ResponseWriter, param, raw string) (uint64, bool) {
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid %s parameter, expected RFC3339 format", param))

		return 0, false
	}

	micros := t.UnixMicro()
	if micros < 0 {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid %s parameter, dates before 1970-01-01 are not supported", param))

		return 0, false
	}

	return uint64(micros), true
}

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
//
// Read-consistency options gap (tracked follow-up): the gRPC ListTransactions
// honours ReadOptions.checkpointId / minLogSequence to pin a read to a
// specific applied index; this HTTP route deliberately does NOT expose them
// and serves a live, best-effort read of the current committed state. Clients
// that need a consistency-bounded / checkpoint-pinned read must use gRPC. This
// mirrors the same carve-out already made for the audit reads (EN-1481) and
// keeps EN-1472 scoped to "expose the reads over HTTP", not "full read-options
// parity". Same applies to the bucket reads (chapters, signing keys) below.
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
			Filter: &commonpb.QueryFilter_BuiltinUint{
				BuiltinUint: &commonpb.BuiltinUintCondition{
					Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
					Cond:  dateCond,
				},
			},
		})
	}

	// The generic `filter` query parameter accepts either the textual filterexpr
	// grammar or the structured v2 JSON DSL (EN-1511); it is AND-combined with the
	// convenience params (reference, startDate/endDate) above.
	generic, ok := parseListFilter(w, r, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
	if !ok {
		return
	}

	filters = append(filters, generic)

	filter := combineFilters(filters...)

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

	writeProtoListOK(w, transactions)
}
