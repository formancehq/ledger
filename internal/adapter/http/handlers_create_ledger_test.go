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

func TestHandleCreateLedger_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_CreateLedger{
							CreateLedger: &commonpb.CreatedLedgerLog{
								Name: "test-ledger",
							},
						},
					},
				},
			}, nil
		})
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger", nil, map[string]string{
		"ledgerName": "test-ledger",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestHandleCreateLedger_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreateLedger_NoLogReturned(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{}, nil
		})
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger", nil, map[string]string{
		"ledgerName": "test-ledger",
	})

	// An apply that returns no log is a backend contract violation; the handler
	// panics (the jsonRecoverer middleware turns this into a 500 in production).
	require.Panics(t, func() {
		srv.handleCreateLedger(w, r)
	})
}

func TestHandleCreateLedger_UnexpectedPayloadType(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_PromoteLedger{
						PromoteLedger: &commonpb.PromotedLedgerLog{Name: "test-ledger"},
					},
				},
			}}, nil
		})
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger", nil, map[string]string{
		"ledgerName": "test-ledger",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleCreateLedger_AlreadyExists(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, &domain.ErrLedgerAlreadyExists{Name: "test-ledger"}
		})
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/test-ledger", nil, map[string]string{
		"ledgerName": "test-ledger",
	})

	srv.handleCreateLedger(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "LEDGER_ALREADY_EXISTS", resp.ErrorCode)
}
