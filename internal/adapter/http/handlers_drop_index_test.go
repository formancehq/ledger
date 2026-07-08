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
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleDropIndex_Success(t *testing.T) {
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
	r := newRequest(t, http.MethodDelete, "/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:color", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "metadata:TARGET_TYPE_ACCOUNT:color",
	})

	srv.handleDropIndex(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
	require.NotNil(t, capturedRequest)
	di, ok := capturedRequest.GetType().(*servicepb.Request_DropIndex)
	require.True(t, ok)
	require.Equal(t, "ledger1", di.DropIndex.GetLedger())
	meta := di.DropIndex.GetId().GetMetadata()
	require.NotNil(t, meta)
	require.Equal(t, "color", meta.GetKey())
}

func TestHandleDropIndex_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP", nil, map[string]string{
		"ledgerName":  "",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleDropIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDropIndex_MissingCanonicalId(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/indexes/", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "",
	})

	srv.handleDropIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDropIndex_InvalidCanonical(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/indexes/bogus-prefix:foo", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "bogus-prefix:foo",
	})

	srv.handleDropIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDropIndex_IdempotencyKeyPropagated(t *testing.T) {
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
	r := newRequest(t, http.MethodDelete, "/ledger1/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})
	r.Header.Set("Idempotency-Key", "drop-index-ik-1")

	srv.handleDropIndex(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
	require.NotNil(t, capturedBatch)
	require.Equal(t, "drop-index-ik-1", capturedBatch.GetIdempotencyKey())
}

func TestHandleDropIndex_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, errors.New("apply failed")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleDropIndex(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
