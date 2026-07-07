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
)

func TestHandleGetAccount_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetAccount(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, addr string) (*commonpb.Account, error) {
			return &commonpb.Account{Address: addr}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts/users:001", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleGetAccount_MissingAddress(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts/", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandleGetAccount_VolumesEmitted asserts that per-asset volumes populated
// upstream (by scanAccount in the controller) survive JSON serialization.
// Regression against the pre-EN-1470 wire, where Account.MarshalJSON explicitly
// dropped the field.
func TestHandleGetAccount_VolumesEmitted(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetAccount(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, addr string) (*commonpb.Account, error) {
			return &commonpb.Account{
				Address: addr,
				Volumes: map[string]*commonpb.VolumesWithBalance{
					"USD/2": {Input: "1000", Output: "400", Balance: "600"},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts/users:001", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	var body struct {
		Data struct {
			Address string `json:"address"`
			Volumes map[string]struct {
				Input   string `json:"input"`
				Output  string `json:"output"`
				Balance string `json:"balance"`
			} `json:"volumes"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))

	require.Equal(t, "users:001", body.Data.Address)
	require.Contains(t, body.Data.Volumes, "USD/2")
	require.Equal(t, "1000", body.Data.Volumes["USD/2"].Input)
	require.Equal(t, "400", body.Data.Volumes["USD/2"].Output)
	require.Equal(t, "600", body.Data.Volumes["USD/2"].Balance)
}

// TestHandleGetAccount_NoVolumesOmitsField asserts that a controller returning
// an account with no `volumes` produces a JSON body without the field
// (`omitempty` on the marshaller).
func TestHandleGetAccount_NoVolumesOmitsField(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return &commonpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetAccount(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, addr string) (*commonpb.Account, error) {
			return &commonpb.Account{Address: addr}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts/users:001", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.NotContains(t, w.Body.String(), `"volumes"`)
}

func TestHandleGetAccount_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*commonpb.LedgerInfo, error) {
			return nil, commonpb.ErrNoLeader
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/missing/accounts/addr", nil, map[string]string{
		"ledgerName": "missing",
		"address":    "addr",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusServiceUnavailable, w.Code)
}
