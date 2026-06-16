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

func TestHandleRemoveAccountType_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/account-types/users", nil, map[string]string{
		"ledgerName": "ledger1",
		"typeName":   "users",
	})

	srv.handleRemoveAccountType(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleRemoveAccountType_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/account-types/users", nil, map[string]string{
		"ledgerName": "",
		"typeName":   "users",
	})

	srv.handleRemoveAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRemoveAccountType_MissingTypeName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/account-types/", nil, map[string]string{
		"ledgerName": "ledger1",
		"typeName":   "",
	})

	srv.handleRemoveAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRemoveAccountType_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return nil, &domain.ErrAccountTypeNotFound{Name: "missing"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/account-types/missing", nil, map[string]string{
		"ledgerName": "ledger1",
		"typeName":   "missing",
	})

	srv.handleRemoveAccountType(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleRemoveAccountType_HasAccounts(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return nil, &domain.ErrAccountTypeHasAccounts{Name: "users"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodDelete, "/ledger1/account-types/users", nil, map[string]string{
		"ledgerName": "ledger1",
		"typeName":   "users",
	})

	srv.handleRemoveAccountType(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
}
