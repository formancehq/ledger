package http

import (
	"fmt"
	"net/http"
	"time"

	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// parseFilterDateMicros parses an RFC3339 date query parameter and returns it
// as Unix microseconds for a UintCondition bound. Builtin date indexes (both
// transaction timestamps and log dates) are stored as unsigned microseconds, so
// a pre-epoch (negative UnixMicro) date has no representable bound: casting it
// to uint64 would wrap to a huge value and silently corrupt the filter (a start
// bound would exclude everything, an end bound would include everything). Such
// dates are rejected with 400 rather than accepted with garbage semantics. On
// error it writes the response and returns ok=false; the caller must return
// immediately.
//
// Shared by handleListTransactions and handleListLedgerLogs so both list
// endpoints validate startDate/endDate identically (EN-1542).
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

// combineFilters AND-combines the non-nil filters into a single QueryFilter.
// Returns nil for zero filters (unfiltered read) and the sole filter unwrapped
// for one, so a single-condition query is not needlessly wrapped in a 1-element
// $and. This is the shared version of the ad-hoc combine logic the list handlers
// used to each carry.
//
// NOTE: it compacts in place over the passed slice's backing array, so a caller
// that spreads a slice (combineFilters(s...)) must treat s as consumed and not
// read it afterward. All current callers do this as their last use of the slice.
func combineFilters(filters ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	compact := filters[:0]
	for _, f := range filters {
		if f != nil {
			compact = append(compact, f)
		}
	}

	switch len(compact) {
	case 0:
		return nil
	case 1:
		return compact[0]
	default:
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{Filters: compact},
			},
		}
	}
}

// parseListFilter decodes the optional `filter` query parameter of a list
// endpoint into a QueryFilter, accepting BOTH the textual filterexpr grammar and
// the structured v2 JSON DSL interchangeably (EN-1511): the value is handed to
// filterexpr.DecodeDualFormat, which detects the form and runs the per-target
// validity gate for `target`. An absent/empty filter yields (nil, true) — an
// unfiltered read. A malformed filter or one carrying a condition invalid on the
// target is a 400; the response is written and ok=false is returned so the
// caller returns immediately.
//
// The structured form is passed over the query string by URL-encoding the JSON
// object as the value (`?filter=%7B%22%24match%22%3A…%7D`); the textual form is
// passed verbatim (`?filter=metadata[k]==v`). Both compile through the same
// downstream path as the endpoint's structured query-param filters, which the
// caller AND-combines via combineFilters.
func parseListFilter(w http.ResponseWriter, r *http.Request, target commonpb.QueryTarget) (*commonpb.QueryFilter, bool) {
	raw := r.URL.Query().Get("filter")
	if raw == "" {
		return nil, true
	}

	filter, err := filterexpr.DecodeDualFormat([]byte(raw), target)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("invalid filter: %w", err))

		return nil, false
	}

	return filter, true
}
