package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleDeleteTransactionMetadata_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/transactions/1/metadata/category", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
		"key":           "category",
	})

	srv.handleDeleteTransactionMetadata(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleDeleteTransactionMetadata_MissingKey(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/transactions/1/metadata/", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
		"key":           "",
	})

	srv.handleDeleteTransactionMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDeleteTransactionMetadata_InvalidTxID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/transactions/abc/metadata/key", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "abc",
		"key":           "key",
	})

	srv.handleDeleteTransactionMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleDeleteTransactionMetadata_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return nil, &domain.ErrMetadataNotFound{Target: "transaction:1", Key: "category"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/transactions/1/metadata/category", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
		"key":           "category",
	})

	srv.handleDeleteTransactionMetadata(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
