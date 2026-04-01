package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

func TestHandleAddAccountType_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(`{"name":"users","pattern":"users:*"}`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestHandleAddAccountType_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/account-types", strings.NewReader(`{"name":"users","pattern":"users:*"}`), map[string]string{
		"ledgerName": "",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddAccountType_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(`{"pattern":"users:*"}`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddAccountType_MissingPattern(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(`{"name":"users"}`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddAccountType_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(`not-json`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddAccountType_AlreadyExists(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrAccountTypeAlreadyExists{Name: "users"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(`{"name":"users","pattern":"users:*"}`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
}
