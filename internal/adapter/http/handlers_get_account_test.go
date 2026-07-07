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
	"github.com/formancehq/ledger/v3/internal/query"
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

// TestHandleGetAccount_ExpandVolumes covers the EN-1470 opt-in: when the
// caller sets `expandVolumes=true`, an AggregateVolumes scan runs on the
// account and its result is folded into `volumes` on the response.
func TestHandleGetAccount_ExpandVolumes(t *testing.T) {
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
	backend.EXPECT().AggregateVolumes(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, filter *commonpb.QueryFilter, _ query.AggregateOptions) (*commonpb.AggregateResult, error) {
			// Sanity-check the filter: exact-match on the requested address.
			require.Equal(t, "users:001", filter.GetAddress().GetHardcodedExact())

			return &commonpb.AggregateResult{
				Volumes: []*commonpb.AggregatedVolume{
					{
						Asset:  "USD/2",
						Input:  commonpb.NewUint256FromUint64(1000),
						Output: commonpb.NewUint256FromUint64(400),
					},
				},
			}, nil
		}).Times(1)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts/users:001?expandVolumes=true", nil, map[string]string{
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

// TestHandleGetAccount_NoExpandVolumesLeavesFieldOff asserts that the default
// read path never populates `volumes` and never calls AggregateVolumes.
func TestHandleGetAccount_NoExpandVolumesLeavesFieldOff(t *testing.T) {
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
	// AggregateVolumes must NOT be called — no EXPECT() means gomock fails if it is.
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
