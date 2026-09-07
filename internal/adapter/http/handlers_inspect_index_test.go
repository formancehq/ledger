package http

import (
	"context"
	"encoding/json"
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

// TestHandleInspectIndex_RendersWithServedBinding pins the render-type source:
// values are typed by the binding that served the scan, not the live declared
// schema. During the legal one-revision retype window the served encoding is
// the predecessor's — an INT64-bound window under a DATETIME schema is raw
// integers, and a DATETIME-bound window renders RFC3339 regardless of any
// later re-declaration.
func TestHandleInspectIndex_RendersWithServedBinding(t *testing.T) {
	t.Parallel()

	render := func(t *testing.T, servedType commonpb.MetadataType) []any {
		t.Helper()

		backend := NewMockBackend(gomock.NewController(t))
		backend.EXPECT().InspectIndex(gomock.Any(), gomock.Any()).Return(
			&servicepb.InspectIndexResponse{
				ServedType: servedType,
				Result: &servicepb.InspectIndexResponse_DistinctValues{
					DistinctValues: &servicepb.InspectDistinctValues{
						Values: []*commonpb.MetadataValue{commonpb.NewIntValue(1700000000000000)},
					},
				},
			}, nil).Times(1)
		srv := newTestServer(t, backend)

		w := httptest.NewRecorder()
		r := newRequest(t, http.MethodGet, "/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:when/inspect?mode=distinct-values", nil, map[string]string{
			"ledgerName":  "ledger1",
			"canonicalId": "metadata:TARGET_TYPE_ACCOUNT:when",
		})

		srv.handleInspectIndex(w, r)
		require.Equal(t, http.StatusOK, w.Code)

		var body struct {
			Data inspectDistinctValuesJSON `json:"data"`
		}
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))

		return body.Data.Values
	}

	intValues := render(t, commonpb.MetadataType_METADATA_TYPE_INT64)
	require.Equal(t, []any{float64(1700000000000000)}, intValues,
		"an INT64-served binding renders raw integers even if the schema has since been retyped")

	dtValues := render(t, commonpb.MetadataType_METADATA_TYPE_DATETIME)
	require.Equal(t, []any{"2023-11-14T22:13:20Z"}, dtValues,
		"a DATETIME-served binding renders RFC3339")
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
