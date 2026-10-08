package http

import (
	"net/http"
	"slices"
	"strings"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
)

// pageQuery is the paging part of a list request: the pageSize, cursor and
// reverse query parameters.
type pageQuery struct {
	size   uint32
	cursor pagecursor.Cursor
	// reverse is the order the source must be read in: the requested order
	// with the cursor's direction applied.
	reverse bool
}

// fetchSize asks the source for one row beyond the page, so a further page
// is detected without a spurious empty round-trip.
func (q pageQuery) fetchSize() uint32 {
	return q.size + 1
}

// parsePageQuery parses pageSize, cursor and reverse, writing a 400 on a
// malformed value.
func parsePageQuery(w http.ResponseWriter, r *http.Request) (pageQuery, bool) {
	size, ok := parsePageSize(w, r)
	if !ok {
		return pageQuery{}, false
	}

	c, err := query.DecodeCursor(r.URL.Query().Get("cursor"))
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return pageQuery{}, false
	}

	return pageQuery{
		size:    size,
		cursor:  c,
		reverse: c.ReadReverse(queryParamBool(r, "reverse")),
	}, true
}

// pageLinks are the adjacent-page tokens of a paged list response. HasMore
// is set iff Next is.
type pageLinks struct {
	Next     string
	Previous string
	HasMore  bool
}

// pagedResponse is the body of a paged list response.
type pagedResponse struct {
	Data     any    `json:"data"`
	Next     string `json:"next,omitempty"`
	Previous string `json:"previous,omitempty"`
	HasMore  bool   `json:"hasMore"`
}

// buildPage turns rows read in q.reverse order into the requested page, in
// query order, with its links. more reports a further row known to the
// source beyond the rows it delivered.
func buildPage[T any](q pageQuery, rows []T, more bool, keyOf func(T) string) ([]T, pageLinks) {
	page, next, previous := pagecursor.Page(q.cursor, rows, q.size, more, keyOf)
	if page == nil {
		page = []T{}
	}

	return page, pageLinks{Next: next, Previous: previous, HasMore: next != ""}
}

// drainPage drains a source read in q.reverse order and builds the page.
// It writes the error response and returns false when the source fails.
func drainPage[T any](w http.ResponseWriter, r *http.Request, q pageQuery, c cursor.Cursor[T], keyOf func(T) string) ([]T, pageLinks, bool) {
	rows, ok := drainCursor(w, r, c)
	if !ok {
		return nil, pageLinks{}, false
	}

	page, links := buildPage(q, rows, cursor.SourceHasMore(c), keyOf)

	return page, links, true
}

// pageSorted serves q from rows already sorted ascending by keyOf, for
// collections small enough to be listed whole.
func pageSorted[T any](q pageQuery, rows []T, keyOf func(T) string) ([]T, pageLinks) {
	if q.reverse {
		rows = slices.Clone(rows)
		slices.Reverse(rows)
	}

	if key := q.cursor.Key; key != "" {
		start := len(rows)

		for i, row := range rows {
			cmp := strings.Compare(keyOf(row), key)
			if q.reverse && cmp < 0 || !q.reverse && cmp > 0 {
				start = i

				break
			}
		}

		rows = rows[start:]
	}

	if uint32(len(rows)) > q.fetchSize() {
		rows = rows[:q.fetchSize()]
	}

	return buildPage(q, rows, false, keyOf)
}

// writePageOK writes a 200 paged list response. The body is marshaled before
// any header is written, so a marshal failure is a clean 500.
func writePageOK(w http.ResponseWriter, r *http.Request, data any, links pageLinks) {
	body, err := json.Marshal(pagedResponse{Data: data, Next: links.Next, Previous: links.Previous, HasMore: links.HasMore})
	if err != nil {
		handleError(w, r, err)

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
