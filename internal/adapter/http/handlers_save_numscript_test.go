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

func TestHandleSaveNumscript_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_SavedNumscript{
							SavedNumscript: &commonpb.SavedNumscriptLog{
								Info: &commonpb.NumscriptInfo{
									Name:    "my-script",
									Version: "1.0.0",
								},
							},
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/numscripts/my-script",
		strings.NewReader(`{"content":"send [USD 100] ( source = @world destination = @alice )","version":"1.0.0"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"name":       "my-script",
		})

	srv.handleSaveNumscript(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

// TestHandleSaveNumscript_LogContractViolations locks in the exact-one typed-log
// contract: a successful save always emits exactly one non-nil SavedNumscript log
// (processSaveNumscript returns either an error or that log). Any other
// cardinality, a nil sole log, or a mismatched payload type must fail loudly
// through unreachable (the jsonRecoverer turns the panic into a sanitized 500 in
// production) — never a silent 2xx.
func TestHandleSaveNumscript_LogContractViolations(t *testing.T) {
	t.Parallel()

	saved := &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SavedNumscript{SavedNumscript: &commonpb.SavedNumscriptLog{
			Info: &commonpb.NumscriptInfo{Name: "my-script", Version: "1.0.0"},
		}},
	}}
	wrongPayload := &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger1"}},
	}}

	cases := []struct {
		name    string
		logs    []*commonpb.Log
		wantMsg string
	}{
		{"zero logs", []*commonpb.Log{}, "apply did not return exactly one log"},
		{"two logs", []*commonpb.Log{saved, saved}, "apply did not return exactly one log"},
		{"nil sole log", []*commonpb.Log{nil}, "apply returned a nil log"},
		{"wrong payload type", []*commonpb.Log{wrongPayload}, "apply returned an unexpected log payload type"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t, backendReturningLogs(t, tc.logs))

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodPut, "/ledger1/numscripts/my-script",
				strings.NewReader(`{"content":"send [USD 100] ( source = @world destination = @alice )","version":"1.0.0"}`),
				map[string]string{
					"ledgerName": "ledger1",
					"name":       "my-script",
				})

			requirePanicsContaining(t, tc.wantMsg, func() {
				srv.handleSaveNumscript(w, r)
			})
		})
	}
}

func TestHandleSaveNumscript_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/numscripts/my-script",
		strings.NewReader(`{"content":"test"}`),
		map[string]string{
			"ledgerName": "",
			"name":       "my-script",
		})

	srv.handleSaveNumscript(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSaveNumscript_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/numscripts/",
		strings.NewReader(`{"content":"test"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"name":       "",
		})

	srv.handleSaveNumscript(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSaveNumscript_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/numscripts/my-script",
		strings.NewReader(`not-json`),
		map[string]string{
			"ledgerName": "ledger1",
			"name":       "my-script",
		})

	srv.handleSaveNumscript(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSaveNumscript_VersionConflict(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, &domain.ErrNumscriptVersionAlreadyExists{Name: "my-script", Version: "1.0.0"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/numscripts/my-script",
		strings.NewReader(`{"content":"test","version":"1.0.0"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"name":       "my-script",
		})

	srv.handleSaveNumscript(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
}
