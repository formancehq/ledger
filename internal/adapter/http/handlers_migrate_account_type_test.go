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

func TestHandleMigrateAccountType_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types/users/migrate",
		strings.NewReader(`{"targetPattern":"users:*:v2"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"typeName":   "users",
		})

	srv.handleMigrateAccountType(w, r)

	require.Equal(t, http.StatusAccepted, w.Code)
}

func TestHandleMigrateAccountType_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/account-types/users/migrate",
		strings.NewReader(`{"targetPattern":"users:*:v2"}`),
		map[string]string{
			"ledgerName": "",
			"typeName":   "users",
		})

	srv.handleMigrateAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleMigrateAccountType_MissingTypeName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types//migrate",
		strings.NewReader(`{"targetPattern":"users:*:v2"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"typeName":   "",
		})

	srv.handleMigrateAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleMigrateAccountType_MissingTargetPattern(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types/users/migrate",
		strings.NewReader(`{}`),
		map[string]string{
			"ledgerName": "ledger1",
			"typeName":   "users",
		})

	srv.handleMigrateAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleMigrateAccountType_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types/users/migrate",
		strings.NewReader(`not-json`),
		map[string]string{
			"ledgerName": "ledger1",
			"typeName":   "users",
		})

	srv.handleMigrateAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleMigrateAccountType_MigrationInProgress(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrAccountTypeMigrationInProgress{Name: "users"}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types/users/migrate",
		strings.NewReader(`{"targetPattern":"users:*:v2"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"typeName":   "users",
		})

	srv.handleMigrateAccountType(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
}
