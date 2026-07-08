package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleGetIndex_Success(t *testing.T) {
	t.Parallel()

	var capturedReq *servicepb.GetIndexRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.GetIndexRequest) (*commonpb.Index, error) {
			capturedReq = req

			return &commonpb.Index{Ledger: req.GetLedger()}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:color", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "metadata:TARGET_TYPE_ACCOUNT:color",
	})

	srv.handleGetIndex(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedReq)
	require.Equal(t, "ledger1", capturedReq.GetLedger())
	require.Equal(t, "color", capturedReq.GetId().GetMetadata().GetKey())
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, capturedReq.GetId().GetMetadata().GetTarget())
}

func TestHandleGetIndex_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.GetIndexRequest) (*commonpb.Index, error) {
			return nil, commonpb.NewNotFoundError("index not found")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleGetIndex(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetIndex_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP", nil, map[string]string{
		"ledgerName":  "",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleGetIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetIndex_InvalidCanonical(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/bogus", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "bogus",
	})

	srv.handleGetIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
