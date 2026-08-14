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
				{
					Id:          1,
					Status:      commonpb.ChapterStatus_CHAPTER_CLOSED,
					SealingHash: []byte{0xde, 0xad, 0xbe, 0xef},
					Start:       &commonpb.Timestamp{Data: 1786540255458491},
				},
				{Id: 2},
			}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/chapters", nil, nil)

	srv.handleListChapters(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// Chapter.MarshalJSON hex-encodes the bytes hash fields and renders the
	// timestamp as RFC3339. protojson would emit base64 ("3q2+7w==") and
	// {"data":"1786540255458491"} instead, because it ignores json.Marshaler.
	body := w.Body.String()
	require.Contains(t, body, `"sealingHash":"deadbeef"`)
	require.NotContains(t, body, "3q2+7w==")
	require.Contains(t, body, `"status":"CHAPTER_CLOSED"`)
	require.NotContains(t, body, `"start":{"data":`)
	require.Contains(t, body, `"start":"2026-08-12T13:10:55.458491Z"`)
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
