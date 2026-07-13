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

func TestHandleInspectIndex_Success(t *testing.T) {
	t.Parallel()

	var capturedReq *servicepb.InspectIndexRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().InspectIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error) {
			capturedReq = req

			return &servicepb.InspectIndexResponse{
				Result: &servicepb.InspectIndexResponse_Summary{
					Summary: &servicepb.InspectSummary{Cardinality: 3},
				},
			}, nil
		}).AnyTimes()
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, name string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: name}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:color/inspect", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "metadata:TARGET_TYPE_ACCOUNT:color",
	})

	srv.handleInspectIndex(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotNil(t, capturedReq)
	require.Equal(t, "ledger1", capturedReq.GetLedger())
	require.Equal(t, "color", capturedReq.GetMetadataKey())
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_ACCOUNT, capturedReq.GetTargetType())
}

func TestHandleInspectIndex_TransactionTarget(t *testing.T) {
	t.Parallel()

	var capturedTarget commonpb.TargetType

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().InspectIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error) {
			capturedTarget = req.GetTargetType()

			return &servicepb.InspectIndexResponse{
				Result: &servicepb.InspectIndexResponse_Summary{Summary: &servicepb.InspectSummary{}},
			}, nil
		}).AnyTimes()
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, name string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: name}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/metadata:TARGET_TYPE_TRANSACTION:category/inspect", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "metadata:TARGET_TYPE_TRANSACTION:category",
	})

	srv.handleInspectIndex(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_TRANSACTION, capturedTarget)
}

func TestHandleInspectIndex_RejectsBuiltinIndex(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP/inspect", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP",
	})

	srv.handleInspectIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Contains(t, w.Body.String(), "metadata")
}

func TestHandleInspectIndex_InvalidCanonical(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/bogus/inspect", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "bogus",
	})

	srv.handleInspectIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleInspectIndex_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/indexes/metadata:TARGET_TYPE_ACCOUNT:color/inspect", nil, map[string]string{
		"ledgerName":  "",
		"canonicalId": "metadata:TARGET_TYPE_ACCOUNT:color",
	})

	srv.handleInspectIndex(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
