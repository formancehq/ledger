package v2

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// upstreamLogFixture models v2.4.7's initial log query: descending by default,
// explicit sort, query filters, and a pageSize+1 lookahead. Unknown parameters
// (notably after) do not change the result. It never infers ordering from size.
type upstreamLogFixture struct {
	mu       sync.Mutex
	logs     []V2Log
	requests []string
	failNext bool
}

func (f *upstreamLogFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.URL.RawQuery)
	if f.failNext {
		f.failNext = false
		http.Error(w, "injected fetch failure", http.StatusServiceUnavailable)

		return
	}
	size, err := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if err != nil || size <= 0 {
		http.Error(w, "invalid pageSize", http.StatusBadRequest)

		return
	}
	var filter struct {
		GT struct {
			ID uint64 `json:"id"`
		} `json:"$gt"`
	}
	if q := r.URL.Query().Get("query"); q != "" {
		if err := json.Unmarshal([]byte(q), &filter); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)

			return
		}
	}
	var logs []V2Log
	for _, log := range f.logs {
		if r.URL.Query().Get("query") == "" || log.ID > filter.GT.ID {
			logs = append(logs, log)
		}
	}
	slices.SortFunc(logs, func(a, b V2Log) int {
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}

		return 0
	})
	if r.URL.Query().Get("sort") != "id:asc" {
		slices.Reverse(logs)
	}
	more := len(logs) > size
	if more {
		logs = logs[:size]
	}
	w.Header().Set("Content-Type", "application/json")
	// A real server also returns opaque next/previous tokens. The adapter's
	// boundary-based request must not depend on a token retained by this fixture.
	if err := json.NewEncoder(w).Encode(V2LogPage{Cursor: V2LogCursor{Data: logs, PageSize: size, HasMore: more}}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func TestHTTPSource_AscendingHistory(t *testing.T) {
	t.Parallel()
	fixture := &upstreamLogFixture{}
	for id := uint64(1); id <= 3; id++ {
		fixture.logs = append(fixture.logs, V2Log{ID: id, Type: "NEW_TRANSACTION", Date: "2024-01-01T00:00:00Z", Data: json.RawMessage(`{"transaction":{"id":` + strconv.FormatUint(id-1, 10) + `,"postings":[{"source":"world","destination":"users:001","amount":100,"asset":"USD/2"}],"timestamp":"2024-01-01T00:00:00Z"}}`)})
	}
	srv := httptest.NewServer(fixture)
	defer srv.Close()
	source := NewHTTPSource(srv.URL, "default", srv.Client())
	ctx := context.Background()
	logs, more, err := source.FetchLogs(ctx, 0, 2)
	require.NoError(t, err)
	require.True(t, more)
	orders, _, _, err := TranslateBatch("mirror", logs, 1, 0, nil)
	require.NoError(t, err)
	// This is the lossy branch: descending [3,2] translates successfully but
	// fabricates gaps for available logs 1 and 2 before replaying the real log 2.
	for _, order := range orders {
		require.Nil(t, order.GetLedgerScoped().GetMirrorIngest().GetEntry().GetFillGap(), "available source history became a synthetic gap; fetched IDs: %v", logIDs(logs))
	}
	require.Equal(t, []uint64{1, 2}, logIDs(logs))

	// A new source instance resumes from the confirmed nonzero boundary.
	source = NewHTTPSource(srv.URL, "default", srv.Client())
	logs, more, err = source.FetchLogs(ctx, 2, 2)
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, []uint64{3}, logIDs(logs))
	logs, more, err = source.FetchLogs(ctx, 3, 2)
	require.NoError(t, err)
	require.False(t, more)
	require.Empty(t, logs)

	fixture.mu.Lock()
	fixture.logs = append(fixture.logs, V2Log{ID: 4})
	fixture.failNext = true
	before := len(fixture.requests)
	fixture.mu.Unlock()
	_, _, err = source.FetchLogs(ctx, 3, 2)
	require.ErrorContains(t, err, "503")
	logs, more, err = source.FetchLogs(ctx, 3, 2)
	require.NoError(t, err)
	require.False(t, more)
	require.Equal(t, []uint64{4}, logIDs(logs))
	fixture.mu.Lock()
	requests := slices.Clone(fixture.requests)
	fixture.mu.Unlock()
	require.Len(t, requests, before+2)
	require.Equal(t, requests[before], requests[before+1], "failed fetch must retry the same boundary")

	latest, err := source.GetLatestLogID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(4), latest)
	logs, more, err = source.FetchLogs(ctx, 0, 1)
	require.NoError(t, err)
	require.True(t, more)
	require.Equal(t, []uint64{1}, logIDs(logs), "pageSize=1 ingestion must differ from a head probe")
}

func logIDs(logs []V2Log) []uint64 {
	ids := make([]uint64, len(logs))
	for i, log := range logs {
		ids[i] = log.ID
	}

	return ids
}

func TestHTTPSource_LargeBoundary(t *testing.T) {
	t.Parallel()
	for _, boundary := range []uint64{1<<53 + 1, ^uint64(0) - 1, ^uint64(0)} {
		t.Run(strconv.FormatUint(boundary, 10), func(t *testing.T) {
			t.Parallel()
			fixture := &upstreamLogFixture{logs: []V2Log{{ID: boundary}}}
			var expected []uint64
			if boundary != ^uint64(0) {
				fixture.logs = append(fixture.logs, V2Log{ID: boundary + 1})
				expected = []uint64{boundary + 1}
			}
			srv := httptest.NewServer(fixture)
			defer srv.Close()
			logs, more, err := NewHTTPSource(srv.URL, "default", srv.Client()).FetchLogs(context.Background(), boundary, 2)
			require.NoError(t, err)
			require.False(t, more)
			if expected == nil {
				require.Empty(t, logs)
			} else {
				require.Equal(t, expected, logIDs(logs))
			}
		})
	}
}

func TestUpstreamLogFixture_DefaultDescendingIgnoresAfter(t *testing.T) {
	t.Parallel()
	fixture := &upstreamLogFixture{logs: []V2Log{{ID: 1}, {ID: 2}, {ID: 3}}}
	for _, query := range []string{"pageSize=2", "pageSize=2&after=2"} {
		recorder := httptest.NewRecorder()
		fixture.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v2/default/logs?"+query, nil))
		require.Equal(t, http.StatusOK, recorder.Code)
		var page V2LogPage
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &page))
		require.Equal(t, []uint64{3, 2}, logIDs(page.Cursor.Data))
		require.True(t, page.Cursor.HasMore)
	}
}
