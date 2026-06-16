package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleListNumscripts_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListNumscripts(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) ([]*commonpb.NumscriptInfo, error) {
			return []*commonpb.NumscriptInfo{
				{Name: "script1", Version: "1.0.0"},
				{Name: "script2", Version: "2.0.0"},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/numscripts", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListNumscripts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListNumscripts_Empty(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListNumscripts(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) ([]*commonpb.NumscriptInfo, error) {
			return nil, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/numscripts", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListNumscripts(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleListNumscripts_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/numscripts", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleListNumscripts(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListNumscripts_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListNumscripts(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) ([]*commonpb.NumscriptInfo, error) {
			return nil, errors.New("unexpected error")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/numscripts", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListNumscripts(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
