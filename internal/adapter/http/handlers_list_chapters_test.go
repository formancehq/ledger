package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListChapters_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListChapters(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.Chapter], error) {
			return cursor.NewSliceCursor([]*commonpb.Chapter{
				{Id: 1},
				{Id: 2},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/chapters", nil, nil)

	srv.handleListChapters(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListChapters_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListChapters(gomock.Any()).DoAndReturn(
		func(_ context.Context) (cursor.Cursor[*commonpb.Chapter], error) {
			return nil, errors.New("boom")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/chapters", nil, nil)

	srv.handleListChapters(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
