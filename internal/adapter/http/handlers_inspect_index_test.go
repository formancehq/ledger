package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleInspectIndex_InvalidModeRouting(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"bogus", "", "SUMMARY"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()

			backend := NewMockBackend(gomock.NewController(t))
			handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodGet, "/v3/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:color/inspect?mode="+mode, nil)

			handler.ServeHTTP(w, r)

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			var body struct {
				ErrorCode string `json:"errorCode"`
			}
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			require.Equal(t, "INVALID_REQUEST", body.ErrorCode)
		})
	}
}

func TestHandleInspectIndex_InvalidPageSizeRouting(t *testing.T) {
	t.Parallel()

	for _, mode := range []string{"summary", "distinctValues", "distinct-values", "facets"} {
		for _, size := range []string{"0", "10001", "", "-1", "abc", "4294967296"} {
			t.Run(mode+"/"+size, func(t *testing.T) {
				t.Parallel()

				backend := NewMockBackend(gomock.NewController(t))
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				w := httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodGet, "/v3/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:color/inspect?mode="+mode+"&pageSize="+size, nil)

				handler.ServeHTTP(w, r)

				require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				var body struct {
					ErrorCode string `json:"errorCode"`
				}
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
				require.Equal(t, "INVALID_PAGE_SIZE", body.ErrorCode)
			})
		}
	}
}

func TestHandleInspectIndex_QueryOptionsRouting(t *testing.T) {
	t.Parallel()

	modes := []struct {
		query string
		want  servicepb.InspectIndexMode
	}{
		{"", servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY},
		{"mode=summary&", servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY},
		{"mode=distinctValues&", servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES},
		{"mode=distinct-values&", servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES},
		{"mode=facets&", servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS},
	}
	sizes := []struct {
		query string
		want  uint32
	}{
		{"", 0},
		{"pageSize=1&", 1},
		{"pageSize=1000&", 1000},
		{"pageSize=1001&", 1001},
		{"pageSize=10000&", 10000},
	}
	for _, mode := range modes {
		for _, size := range sizes {
			t.Run(mode.query+size.query, func(t *testing.T) {
				t.Parallel()

				backend := NewMockBackend(gomock.NewController(t))
				backend.EXPECT().InspectIndex(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error) {
						require.Equal(t, "ledger1", req.GetLedger())
						require.Equal(t, ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, req.GetTargetType())
						require.Equal(t, "color", req.GetMetadataKey())
						require.Equal(t, mode.want, req.GetMode())
						require.Equal(t, size.want, req.GetPageSize())
						require.Equal(t, "page-token", req.GetCursor())

						return &servicepb.InspectIndexResponse{
							Result: &servicepb.InspectIndexResponse_Summary{Summary: &servicepb.InspectSummary{}},
						}, nil
					})
				backend.EXPECT().GetLedgerByName(gomock.Any(), "ledger1").Return(&ledgerpb.LedgerInfo{Name: "ledger1"}, nil)
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				w := httptest.NewRecorder()
				r := httptest.NewRequest(http.MethodGet, "/v3/ledger1/indexes/metadata:TARGET_TYPE_ACCOUNT:color/inspect?"+mode.query+size.query+"cursor=page-token", nil)

				handler.ServeHTTP(w, r)

				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			})
		}
	}
}

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
		func(_ context.Context, name string) (*ledgerpb.LedgerInfo, error) {
			return &ledgerpb.LedgerInfo{Name: name}, nil
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
	require.Equal(t, ledgerpb.TargetType_TARGET_TYPE_ACCOUNT, capturedReq.GetTargetType())
}

func TestHandleInspectIndex_TransactionTarget(t *testing.T) {
	t.Parallel()

	var capturedTarget ledgerpb.TargetType

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().InspectIndex(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error) {
			capturedTarget = req.GetTargetType()

			return &servicepb.InspectIndexResponse{
				Result: &servicepb.InspectIndexResponse_Summary{Summary: &servicepb.InspectSummary{}},
			}, nil
		}).AnyTimes()
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, name string) (*ledgerpb.LedgerInfo, error) {
			return &ledgerpb.LedgerInfo{Name: name}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/indexes/metadata:TARGET_TYPE_TRANSACTION:category/inspect", nil, map[string]string{
		"ledgerName":  "ledger1",
		"canonicalId": "metadata:TARGET_TYPE_TRANSACTION:category",
	})

	srv.handleInspectIndex(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, ledgerpb.TargetType_TARGET_TYPE_TRANSACTION, capturedTarget)
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
