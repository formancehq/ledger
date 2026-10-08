package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	protoerr "github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetAccount_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*ledgerpb.LedgerInfo, error) {
			return &ledgerpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetAccount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, addr string, _ ctrl.GetAccountOptions) (*ledgerpb.Account, error) {
			return &ledgerpb.Account{Address: addr}, nil
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

// TestHandleGetAccount_PropagatesCollapseColors pins that the HTTP handler
// forwards `?collapseColors=true` through to the backend as
// GetAccountOptions.CollapseColors=true. A regression where the handler stops
// parsing or forwarding the field would fail the matcher on EXPECT().GetAccount.
func TestHandleGetAccount_PropagatesCollapseColors(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*ledgerpb.LedgerInfo, error) {
			return &ledgerpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	// The matcher rejects the call if opts.CollapseColors is not true.
	backend.EXPECT().GetAccount(gomock.Any(), gomock.Any(), gomock.Any(), ctrl.GetAccountOptions{CollapseColors: true}).
		DoAndReturn(func(_ context.Context, _ string, addr string, _ ctrl.GetAccountOptions) (*ledgerpb.Account, error) {
			return &ledgerpb.Account{Address: addr}, nil
		})
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts/alice?collapseColors=true", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "alice",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

// TestHandleGetAccount_VolumesInJSON pins that Account.MarshalJSON emits the
// Volumes array with color:"" for the uncolored bucket. Without this
// assertion a marshaller regression that drops the Volumes field (the
// previous behaviour) would slip through, since the handler test only
// looks at the status code.
func TestHandleGetAccount_VolumesInJSON(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*ledgerpb.LedgerInfo, error) {
			return &ledgerpb.LedgerInfo{Name: "ledger1"}, nil
		}).AnyTimes()
	backend.EXPECT().GetAccount(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, addr string, _ ctrl.GetAccountOptions) (*ledgerpb.Account, error) {
			return &ledgerpb.Account{
				Address: addr,
				Volumes: []*ledgerpb.AccountVolume{
					{
						Asset: "USD/2",
						Color: "", // uncolored bucket — must appear in JSON with color:""
						Volumes: &ledgerpb.VolumesWithBalance{
							Input:   "100",
							Output:  "30",
							Balance: "70",
						},
					},
					{
						Asset: "USD/2",
						Color: "GRANTS",
						Volumes: &ledgerpb.VolumesWithBalance{
							Input:   "50",
							Output:  "0",
							Balance: "50",
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/accounts/alice", nil, map[string]string{
		"ledgerName": "ledger1",
		"address":    "alice",
	})

	srv.handleGetAccount(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	body := w.Body.String()
	require.Contains(t, body, `"volumes":[`, "Account JSON must include the volumes array")
	require.Contains(t, body, `"asset":"USD/2"`)
	require.Contains(t, body, `"color":""`, "the uncolored bucket must surface as color:\"\" not be omitted")
	require.Contains(t, body, `"color":"GRANTS"`)
	require.Contains(t, body, `"balance":"70"`)
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

func TestHandleGetAccount_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLedgerByName(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string) (*ledgerpb.LedgerInfo, error) {
			return nil, protoerr.ErrNoLeader
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
