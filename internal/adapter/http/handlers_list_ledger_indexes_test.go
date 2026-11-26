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
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleListLedgerIndexes_Success(t *testing.T) {
	t.Parallel()

	var capturedReq *servicepb.ListIndexesRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListIndexes(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ListIndexesRequest) (cursor.Cursor[*commonpb.Index], error) {
			capturedReq = req

			return cursor.NewSliceCursor([]*commonpb.Index{{}}), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerIndexes(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedReq)
	require.Equal(t, servicepb.ListIndexesRequest_SCOPE_LEDGER, capturedReq.GetScope())
	require.Equal(t, "ledger1", capturedReq.GetLedger())
}

func TestHandleListLedgerIndexes_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes", nil, map[string]string{"ledgerName": ""})

	srv.handleListLedgerIndexes(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleListLedgerIndexes_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListIndexes(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ListIndexesRequest) (cursor.Cursor[*commonpb.Index], error) {
			return nil, errors.New("boom")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleListLedgerIndexes(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
