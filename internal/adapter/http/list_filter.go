package http

import (
	"fmt"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// combineFilters AND-combines the non-nil filters into a single QueryFilter.
// Returns nil for zero filters (unfiltered read) and the sole filter unwrapped
// for one, so a single-condition query is not needlessly wrapped in a 1-element
// $and. This is the shared version of the ad-hoc combine logic the list handlers
// used to each carry.
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
