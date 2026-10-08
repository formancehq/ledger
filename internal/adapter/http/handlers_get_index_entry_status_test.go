package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	protoerr "github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetIndexEntryStatus_Success(t *testing.T) {
	t.Parallel()

	var capturedReq *ledgerpb.GetIndexEntryStatusRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndexEntryStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *ledgerpb.GetIndexEntryStatusRequest) (*ledgerpb.IndexEntry, error) {
			capturedReq = req

			return &ledgerpb.IndexEntry{Ledger: req.GetLedger(), CurrentVersion: 3}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:color/status", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "metadata:TARGET_TYPE_ACCOUNT:color",
	})

	srv.handleGetIndexEntryStatus(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedReq)
	require.Equal(t, "ledger1", capturedReq.GetLedger())
	require.Equal(t, "color", capturedReq.GetId().GetMetadata().GetKey())
}

func TestHandleGetIndexEntryStatus_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetIndexEntryStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *ledgerpb.GetIndexEntryStatusRequest) (*ledgerpb.IndexEntry, error) {
			return nil, protoerr.NewNotFoundError("not found")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP/status", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleGetIndexEntryStatus(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleGetIndexEntryStatus_InvalidCanonical(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/bogus/status", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "bogus",
	})

	srv.handleGetIndexEntryStatus(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetIndexEntryStatus_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP/status", nil, map[string]string{
		"ledgerName":  "",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleGetIndexEntryStatus(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
