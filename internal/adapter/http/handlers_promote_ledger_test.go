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

func TestHandlePromoteLedger_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_PromoteLedger{
						PromoteLedger: &commonpb.PromotedLedgerLog{
							Name: "mirror-ledger",
						},
					},
				},
			}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/mirror-ledger/promote", nil, map[string]string{
		"ledgerName": "mirror-ledger",
	})

	srv.handlePromoteLedger(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestHandlePromoteLedger_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/promote", nil, map[string]string{
		"ledgerName": "",
	})

	srv.handlePromoteLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandlePromoteLedger_NotMirrorMode(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, &domain.ErrLedgerNotInMirrorMode{Name: "normal-ledger"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/normal-ledger/promote", nil, map[string]string{
		"ledgerName": "normal-ledger",
	})

	srv.handlePromoteLedger(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "LEDGER_NOT_IN_MIRROR_MODE", resp.ErrorCode)
}

func TestHandlePromoteLedger_NoLogReturned(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/mirror-ledger/promote", nil, map[string]string{
		"ledgerName": "mirror-ledger",
	})

	// An apply that returns no log is a backend contract violation; the handler
	// panics (the jsonRecoverer middleware turns this into a 500 in production).
	require.Panics(t, func() {
		srv.handlePromoteLedger(w, r)
	})
}

func TestHandlePromoteLedger_UnexpectedPayloadType(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_CreateLedger{
						CreateLedger: &commonpb.CreatedLedgerLog{Name: "mirror-ledger"},
					},
				},
			}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/mirror-ledger/promote", nil, map[string]string{
		"ledgerName": "mirror-ledger",
	})

	srv.handlePromoteLedger(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandlePromoteLedger_LedgerNotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, &domain.ErrLedgerNotFound{Name: "missing"}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/missing/promote", nil, map[string]string{
		"ledgerName": "missing",
	})

	srv.handlePromoteLedger(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
