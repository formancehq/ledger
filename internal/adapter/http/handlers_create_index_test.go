package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleCreateIndex_Success(t *testing.T) {
	t.Parallel()

	var capturedRequest *servicepb.Request

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			capturedRequest = req.GetUnsigned().GetRequests()[0]

			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"id":"metadata:TARGET_TYPE_ACCOUNT:color"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/indexes", body, map[string]string{
		"ledgerName": "ledger1",
	})
	r.ContentLength = int64(len(`{"id":"metadata:TARGET_TYPE_ACCOUNT:color"}`))

	srv.handleCreateIndex(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, capturedRequest)
	ci, ok := capturedRequest.GetType().(*servicepb.Request_CreateIndex)
	require.True(t, ok)
	require.Equal(t, "ledger1", ci.CreateIndex.GetLedger())
	meta := ci.CreateIndex.GetId().GetMetadata()
	require.NotNil(t, meta)
	require.Equal(t, "color", meta.GetKey())
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, meta.GetTarget())
}

func TestHandleCreateIndex_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"id":"tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP"}`)
	r := newRequest(t, http.MethodPost, "/indexes", body, map[string]string{"ledgerName": ""})

	srv.handleCreateIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreateIndex_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`not json`)
	r := newRequest(t, http.MethodPost, "/ledger1/indexes", body, map[string]string{
		"ledgerName": "ledger1",
	})
	r.ContentLength = int64(len("not json"))

	srv.handleCreateIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "invalid request body")
}

func TestHandleCreateIndex_EmptyID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"id":""}`)
	r := newRequest(t, http.MethodPost, "/ledger1/indexes", body, map[string]string{
		"ledgerName": "ledger1",
	})
	r.ContentLength = int64(len(`{"id":""}`))

	srv.handleCreateIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreateIndex_InvalidCanonical(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"id":"bogus-prefix:foo"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/indexes", body, map[string]string{
		"ledgerName": "ledger1",
	})
	r.ContentLength = int64(len(`{"id":"bogus-prefix:foo"}`))

	srv.handleCreateIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreateIndex_IdempotencyKeyPropagated(t *testing.T) {
	t.Parallel()

	var capturedBatch *servicepb.ApplyBatch

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			capturedBatch = req.GetUnsigned()

			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"id":"tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/indexes", body, map[string]string{
		"ledgerName": "ledger1",
	})
	r.ContentLength = int64(len(`{"id":"tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP"}`))
	r.Header.Set("Idempotency-Key", "create-index-ik-1")

	srv.handleCreateIndex(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, capturedBatch)
	require.Equal(t, "create-index-ik-1", capturedBatch.GetIdempotencyKey())
}

func TestHandleCreateIndex_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, errors.New("apply failed")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"id":"tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/indexes", body, map[string]string{
		"ledgerName": "ledger1",
	})
	r.ContentLength = int64(len(`{"id":"tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP"}`))

	srv.handleCreateIndex(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
