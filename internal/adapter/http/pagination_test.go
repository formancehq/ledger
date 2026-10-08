package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
)

type pagedBody struct {
	Data []struct {
		Address string `json:"address"`
	} `json:"data"`
	Next     string `json:"next"`
	Previous string `json:"previous"`
	HasMore  bool   `json:"hasMore"`
}

func decodeToken(t *testing.T, token string) pagecursor.Cursor {
	t.Helper()

	c, err := pagecursor.Decode(token)
	require.NoError(t, err)

	return c
}

func TestHandleListAccounts_PageLinks(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAccounts(gomock.Any(), "ledger1", uint32(3), "a", gomock.Any(), false).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			return cursor.NewSliceCursor([]*commonpb.Account{{Address: "b"}, {Address: "c"}, {Address: "d"}}), nil
		}).Times(1)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts?pageSize=2&cursor="+pagecursor.Cursor{Key: "a"}.Encode(), nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	var body pagedBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body.Data, 2)
	require.Equal(t, "b", body.Data[0].Address)
	require.Equal(t, "c", body.Data[1].Address)
	require.True(t, body.HasMore)
	require.Equal(t, pagecursor.Cursor{Key: "c"}, decodeToken(t, body.Next))
	require.Equal(t, pagecursor.Cursor{Key: "b", Back: true}, decodeToken(t, body.Previous))
}

func TestHandleListAccounts_BackPage(t *testing.T) {
	t.Parallel()

	// A back page reads the opposite order from its key and is returned in
	// query order.
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListAccounts(gomock.Any(), "ledger1", uint32(3), "d", gomock.Any(), true).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, _ string, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*commonpb.Account], error) {
			return cursor.NewSliceCursor([]*commonpb.Account{{Address: "c"}, {Address: "b"}}), nil
		}).Times(1)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts?pageSize=2&cursor="+pagecursor.Cursor{Key: "d", Back: true}.Encode(), nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	var body pagedBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body.Data, 2)
	require.Equal(t, "b", body.Data[0].Address)
	require.Equal(t, "c", body.Data[1].Address)
	require.Equal(t, pagecursor.Cursor{Key: "c"}, decodeToken(t, body.Next))
	require.Empty(t, body.Previous, "no row before b was read")
}

func TestHandleListAccounts_InvalidCursor(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts?cursor=notacursor", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListAccounts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPageSorted(t *testing.T) {
	t.Parallel()

	rows := []string{"a", "b", "c", "d", "e"}
	key := func(s string) string { return s }

	for _, tc := range []struct {
		name     string
		query    pageQuery
		page     []string
		next     *pagecursor.Cursor
		previous *pagecursor.Cursor
	}{
		{
			name:  "first page",
			query: pageQuery{size: 2},
			page:  []string{"a", "b"},
			next:  &pagecursor.Cursor{Key: "b"},
		},
		{
			name:     "resumed page",
			query:    pageQuery{size: 2, cursor: pagecursor.Cursor{Key: "b"}},
			page:     []string{"c", "d"},
			next:     &pagecursor.Cursor{Key: "d"},
			previous: &pagecursor.Cursor{Key: "c", Back: true},
		},
		{
			name:     "back page",
			query:    pageQuery{size: 2, cursor: pagecursor.Cursor{Key: "c", Back: true}, reverse: true},
			page:     []string{"a", "b"},
			next:     &pagecursor.Cursor{Key: "b"},
			previous: nil,
		},
		{
			name:     "last page",
			query:    pageQuery{size: 2, cursor: pagecursor.Cursor{Back: true}, reverse: true},
			page:     []string{"d", "e"},
			previous: &pagecursor.Cursor{Key: "d", Back: true},
		},
		{
			name:     "reverse order",
			query:    pageQuery{size: 2, cursor: pagecursor.Cursor{Key: "d"}, reverse: true},
			page:     []string{"c", "b"},
			next:     &pagecursor.Cursor{Key: "b"},
			previous: &pagecursor.Cursor{Key: "c", Back: true},
		},
		{
			name:     "cursor past the end",
			query:    pageQuery{size: 2, cursor: pagecursor.Cursor{Key: "z"}},
			page:     []string{},
			previous: &pagecursor.Cursor{Back: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			page, links := pageSorted(tc.query, rows, key)
			require.Equal(t, tc.page, page)

			if tc.next == nil {
				require.Empty(t, links.Next)
			} else {
				require.Equal(t, *tc.next, decodeToken(t, links.Next))
			}

			require.Equal(t, links.Next != "", links.HasMore)

			if tc.previous == nil {
				require.Empty(t, links.Previous)
			} else {
				require.Equal(t, *tc.previous, decodeToken(t, links.Previous))
			}
		})
	}

	require.Equal(t, []string{"a", "b", "c", "d", "e"}, rows, "pageSorted must not reorder its input")
}
