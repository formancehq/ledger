package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/stretchr/testify/require"
)

func TestHandleSetMetadataType_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"type":"int64"}`)
	r := newRequest(t, http.MethodPut, "/ledger1/metadata-schema/account/age", body, map[string]string{
		"ledgerName": "ledger1",
		"targetType": "account",
		"key":        "age",
	})

	srv.handleSetMetadataType(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleSetMetadataType_InvalidTargetType(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"type":"string"}`)
	r := newRequest(t, http.MethodPut, "/ledger1/metadata-schema/invalid/key", body, map[string]string{
		"ledgerName": "ledger1",
		"targetType": "invalid",
		"key":        "key",
	})

	srv.handleSetMetadataType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetMetadataType_InvalidMetadataType(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"type":"badtype"}`)
	r := newRequest(t, http.MethodPut, "/ledger1/metadata-schema/account/key", body, map[string]string{
		"ledgerName": "ledger1",
		"targetType": "account",
		"key":        "key",
	})

	srv.handleSetMetadataType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetMetadataType_MissingKey(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"type":"string"}`)
	r := newRequest(t, http.MethodPut, "/ledger1/metadata-schema/account/", body, map[string]string{
		"ledgerName": "ledger1",
		"targetType": "account",
		"key":        "",
	})

	srv.handleSetMetadataType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSetMetadataType_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	body := strings.NewReader(`not json`)
	r := newRequest(t, http.MethodPut, "/ledger1/metadata-schema/account/key", body, map[string]string{
		"ledgerName": "ledger1",
		"targetType": "account",
		"key":        "key",
	})

	srv.handleSetMetadataType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
