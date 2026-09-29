package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	protoerr "github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetMetadataSchema_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetMetadataSchemaStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.GetMetadataSchemaStatusResponse, error) {
			return &commonpb.GetMetadataSchemaStatusResponse{
				AccountFields: map[string]*commonpb.MetadataFieldStatus{
					"role": {DeclaredType: commonpb.MetadataType_METADATA_TYPE_STRING},
				},
				TransactionFields: map[string]*commonpb.MetadataFieldStatus{},
				LedgerFields: map[string]*commonpb.MetadataFieldStatus{
					"env": {DeclaredType: commonpb.MetadataType_METADATA_TYPE_STRING},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/metadata-schema", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleGetMetadataSchema(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{
		"data": {
			"accountFields": {"role": {"declaredType": "string"}},
			"transactionFields": {},
			"ledgerFields": {"env": {"declaredType": "string"}}
		}
	}`, w.Body.String())
}

func TestHandleGetMetadataSchema_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/metadata-schema", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleGetMetadataSchema(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetMetadataSchema_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetMetadataSchemaStatus(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.GetMetadataSchemaStatusResponse, error) {
			return nil, protoerr.ErrNoLeader
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/metadata-schema", nil, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleGetMetadataSchema(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}
