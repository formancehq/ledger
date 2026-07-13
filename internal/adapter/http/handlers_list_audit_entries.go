package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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
//   - after:    exclusive lower bound on the audit sequence (opaque cursor from
//     a previous page; a decimal uint64, matching the gRPC cursor)
//   - reverse:  iterate newest-first when "true"
//   - filter:   filterexpr DSL restricted to audit[...] conditions, e.g.
//     `audit[outcome] == failure`, `audit[ledger] == main`,
//     `audit[order_type] in (create_transaction, revert_transaction)`.
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
// additionally honors the read-consistency options `checkpointId` (pinned
// checkpoint read) and `minLogSequence` (audit-index catch-up wait for filtered
// reads). This HTTP endpoint intentionally does not expose either — it always
// performs a live, best-effort read. A filtered live read therefore resolves
// through the async audit secondary index and may transiently omit very recent
// entries that have not yet been indexed; a client needing a pinned or
// consistency-bounded audit read must use the gRPC surface. If these options
// are added to HTTP later, wire them through the same controller entry points
// the gRPC path uses (impl.openCheckpointStores / minLogSequence gating).
func (s *Server) handleListAuditEntries(w http.ResponseWriter, r *http.Request) {
	pageSize, ok := parsePageSize(w, r)
	if !ok {
		return
	}

	var afterSequence uint64
	if after := r.URL.Query().Get("after"); after != "" {
		parsed, err := strconv.ParseUint(after, 10, 64)
		if err != nil {
			writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid after parameter"))

			return
		}

		afterSequence = parsed
	}

	filter, ok := parseAuditFilter(w, r)
	if !ok {
		return
	}

	reverse := queryParamBool(r, "reverse")

	cursor, err := s.backend.ListAuditEntries(r.Context(), pageSize, afterSequence, filter, reverse)
	if err != nil {
		handleError(w, r, err)

		return
	}

	entries, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	// writeOKChecked (not writeOK): audit DTOs marshal chain-bound submessages
	// via protojson, which can fail; buffering before the header keeps a marshal
	// failure a clean 500 instead of a truncated 200 body.
	writeOKChecked(w, r, entries)
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
