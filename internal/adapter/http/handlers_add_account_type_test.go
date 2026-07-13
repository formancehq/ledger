package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleAddAccountType_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
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

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/account-types", strings.NewReader(`{"name":"users","pattern":"users:*"}`), map[string]string{
		"ledgerName": "",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddAccountType_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(`{"pattern":"users:*"}`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddAccountType_MissingPattern(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(`{"name":"users"}`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddAccountType_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(`not-json`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddAccountType_FullModel(t *testing.T) {
	t.Parallel()

	var captured *commonpb.AccountType

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			reqs := req.GetUnsigned().GetRequests()
			require.Len(t, reqs, 1)
			captured = reqs[0].GetAddAccountType().GetAccountType()

			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	body := `{
		"name": "user-checking",
		"pattern": "users:{id}:checking",
		"persistence": "TRANSIENT",
		"segmentTypes": {
			"id": {"type": "uuid"}
		}
	}`

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(body), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, captured)
	require.Equal(t, "user-checking", captured.GetName())
	require.Equal(t, "users:{id}:checking", captured.GetPattern())
	require.Equal(t, commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT, captured.GetPersistence())

	seg, ok := captured.GetSegmentTypes()["id"]
	require.True(t, ok)
	require.IsType(t, &commonpb.SegmentType_Uuid{}, seg.GetConstraint())
}

func TestHandleAddAccountType_RegexSegmentConstraint(t *testing.T) {
	t.Parallel()

	var captured *commonpb.AccountType

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			captured = req.GetUnsigned().GetRequests()[0].GetAddAccountType().GetAccountType()

			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	body := `{
		"name": "bank-main",
		"pattern": "banks:{iban}:main",
		"segmentTypes": {
			"iban": {"type": "regex", "regex": "[A-Z]{2}[0-9]{14}"}
		}
	}`

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(body), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, captured)
	// Persistence defaults to NORMAL when omitted.
	require.Equal(t, commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL, captured.GetPersistence())

	seg := captured.GetSegmentTypes()["iban"]
	regex, ok := seg.GetConstraint().(*commonpb.SegmentType_Regex)
	require.True(t, ok)
	require.Equal(t, "[A-Z]{2}[0-9]{14}", regex.Regex)
}

func TestHandleAddAccountType_InvalidPersistence(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types",
		strings.NewReader(`{"name":"users","pattern":"users:*","persistence":"BOGUS"}`), map[string]string{
			"ledgerName": "ledger1",
		})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddAccountType_InvalidSegmentType(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types",
		strings.NewReader(`{"name":"users","pattern":"users:{id}","segmentTypes":{"id":{"type":"nope"}}}`), map[string]string{
			"ledgerName": "ledger1",
		})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleAddAccountType_AlreadyExists(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, &domain.ErrAccountTypeAlreadyExists{Name: "users"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/account-types", strings.NewReader(`{"name":"users","pattern":"users:*"}`), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleAddAccountType(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
}
