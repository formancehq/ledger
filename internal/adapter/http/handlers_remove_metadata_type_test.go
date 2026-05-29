package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleRemoveMetadataType_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/metadata-schema/account/age", nil, map[string]string{
		"ledgerName": "ledger1",
		"targetType": "account",
		"key":        "age",
	})

	srv.handleRemoveMetadataType(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleRemoveMetadataType_InvalidTargetType(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/metadata-schema/invalid/key", nil, map[string]string{
		"ledgerName": "ledger1",
		"targetType": "invalid",
		"key":        "key",
	})

	srv.handleRemoveMetadataType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRemoveMetadataType_MissingKey(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/metadata-schema/account/", nil, map[string]string{
		"ledgerName": "ledger1",
		"targetType": "account",
		"key":        "",
	})

	srv.handleRemoveMetadataType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
