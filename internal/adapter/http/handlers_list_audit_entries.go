package http

import (
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// handleListAuditEntries handles GET /v3/_/audit-entries.
//
// Audit is a cluster/bucket-wide read (not ledger-scoped in the path): a single
// proposal can touch several ledgers, so audit entries are addressed by their
// global sequence rather than under a {ledgerName}. Ledger scope, outcome and
// the other audit dimensions are expressed through the `filter` query parameter.
//
// Query parameters:
//   - pageSize: max entries per page (default 100, capped at 1000)
//   - cursor:   page token from a previous response's next / previous; its
//     key is the decimal audit sequence
//   - reverse:  iterate newest-first when "true"
//   - filter:   filterexpr DSL restricted to bare audit fields, e.g.
//     `outcome == failure`, `ledger == main`,
//     `order_type in (create_transaction, revert_transaction)`.
//
// This exposes the same audit data and filter representation as the gRPC
// BucketService.ListAuditEntries surface (EN-1241): it consumes the same
// controller path and the same filter grammar. The filter uses the shared
// filterexpr grammar — the same one ledgerctl feeds to --filter — rather than
// the REST-JSON QueryFilter codec, which deliberately rejects audit conditions
// (their field names collide with transaction/log conditions in the JSON DSL;
// see commonpb/query_filter.go).
//
// It is NOT a full parity of the gRPC ListOptions contract: the gRPC surface
// additionally honors `checkpointId` for a pinned checkpoint read. This HTTP
// endpoint always performs a live read, linearizable unless the caller sends
// `X-Consistency: stale` (see readConsistency). For indexed filters, the
// shared controller path automatically waits for the audit projection to
// certify the fixed main-store horizon before serving the result; unfiltered
// and seq-only reads do not depend on that projection. If checkpoint selection
// is added to HTTP later, wire it through the same controller entry points the
// gRPC path uses (impl.openCheckpointStores / Raft-horizon gating).
func (s *Server) handleListAuditEntries(w http.ResponseWriter, r *http.Request) {
	page, ok := parsePageQuery(w, r)
	if !ok {
		return
	}

	afterSequence, err := query.CursorUint64(page.cursor)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	filter, ok := parseAuditFilter(w, r)
	if !ok {
		return
	}

	cursor, err := s.backend.ListAuditEntries(r.Context(), page.fetchSize(), afterSequence, filter, page.reverse)
	if err != nil {
		handleError(w, r, err)

		return
	}

	entries, links, ok := drainPage(w, r, page, cursor, func(e *auditpb.AuditEntry) string {
		return strconv.FormatUint(e.GetSequence(), 10)
	})
	if !ok {
		return
	}

	// Audit DTOs marshal chain-bound submessages via protojson, which can
	// fail; writePageOK buffers before the header so a marshal failure stays a
	// clean 500 instead of a truncated 200 body.
	writePageOK(w, r, entries, links)
}

// parseAuditFilter parses the optional `filter` query parameter into a
// QueryFilter via the shared dual-format decoder (EN-1511), the same one every
// other list endpoint uses. An absent filter yields a nil filter (unfiltered
// read). A malformed filter, or one carrying a condition invalid on the AUDIT
// target, is a 400.
//
// In practice audit filters are textual: the structured v2 JSON DSL has no
// representation for audit conditions (their field names collide with the
// transaction/log conditions the codec already claims — EN-1241), so the codec
// rejects them. The decoder still accepts the structured form as input; it just
// cannot carry an audit condition, so the textual form is the canonical one for
// this endpoint. See the handler doc above and commonpb/query_filter.go.
func parseAuditFilter(w http.ResponseWriter, r *http.Request) (*commonpb.QueryFilter, bool) {
	return parseListFilter(w, r, commonpb.QueryTarget_QUERY_TARGET_AUDIT)
}
