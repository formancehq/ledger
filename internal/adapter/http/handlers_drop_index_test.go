package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
)

func TestHandleDropIndex_Success(t *testing.T) {
	t.Parallel()

	var capturedRequest *ledgerpb.Request

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *ledgerpb.ApplyRequest) (*domain.ApplyResult, error) {
			capturedRequest = req.GetUnsigned().GetRequests()[0]

			return &domain.ApplyResult{Logs: []*ledgerpb.Log{{}}}, nil
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
	di, ok := capturedRequest.GetType().(*ledgerpb.Request_DropIndex)
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

	var capturedBatch *ledgerpb.ApplyBatch

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *ledgerpb.ApplyRequest) (*domain.ApplyResult, error) {
			capturedBatch = req.GetUnsigned()

			return &domain.ApplyResult{Logs: []*ledgerpb.Log{{}}}, nil
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
		func(_ context.Context, _ *ledgerpb.ApplyRequest) (*domain.ApplyResult, error) {
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
