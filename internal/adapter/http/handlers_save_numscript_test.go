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
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
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

func TestHandleSaveNumscript_NoLogReturned(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return nil, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/numscripts/my-script",
		strings.NewReader(`{"content":"send [USD 100] ( source = @world destination = @alice )"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"name":       "my-script",
		})

	// A successful save always emits a log (processSaveNumscript returns either an
	// error or a SavedNumscript log), so no log is a backend contract violation:
	// the handler panics (jsonRecoverer turns this into a 500 in production).
	require.Panics(t, func() {
		srv.handleSaveNumscript(w, r)
	})
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
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
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
