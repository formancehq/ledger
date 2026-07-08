package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestHandleGetChapterSchedule_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetChapterSchedule(gomock.Any()).DoAndReturn(
		func(_ context.Context) (string, error) {
			return "0 0 * * *", nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/chapter-schedule", nil, nil)

	srv.handleGetChapterSchedule(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"schedule":"0 0 * * *"`)
}

func TestHandleGetChapterSchedule_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetChapterSchedule(gomock.Any()).DoAndReturn(
		func(_ context.Context) (string, error) {
			return "", errors.New("boom")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/chapter-schedule", nil, nil)

	srv.handleGetChapterSchedule(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
