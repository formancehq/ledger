package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleSaveNumscript_Success(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
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
		},
	}
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

func TestHandleSaveNumscript_NoContent(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPut, "/ledger1/numscripts/my-script",
		strings.NewReader(`{"content":"send [USD 100] ( source = @world destination = @alice )"}`),
		map[string]string{
			"ledgerName": "ledger1",
			"name":       "my-script",
		})

	srv.handleSaveNumscript(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleSaveNumscript_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, &mockBackend{})

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

	srv := newTestServer(t, &mockBackend{})

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

	srv := newTestServer(t, &mockBackend{})

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

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return nil, &domain.ErrNumscriptVersionAlreadyExists{Name: "my-script", Version: "1.0.0"}
		},
	}
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
