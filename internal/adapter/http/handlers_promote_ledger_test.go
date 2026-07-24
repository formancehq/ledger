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

// TestHandlePromoteLedger_LogContractViolations locks in the exact-one
// typed-log contract for promote: exactly one non-nil PromoteLedger log. Any
// other cardinality, a nil sole log, or a mismatched payload type must fail
// loudly through unreachable (the jsonRecoverer turns the panic into a
// sanitized 500 in production).
func TestHandlePromoteLedger_LogContractViolations(t *testing.T) {
	t.Parallel()

	promoted := &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_PromoteLedger{PromoteLedger: &commonpb.PromotedLedgerLog{Name: "mirror-ledger"}},
	}}
	wrongPayload := &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "mirror-ledger"}},
	}}

	cases := []struct {
		name string
		logs []*commonpb.Log
	}{
		{"zero logs", []*commonpb.Log{}},
		{"two logs", []*commonpb.Log{promoted, promoted}},
		{"nil sole log", []*commonpb.Log{nil}},
		{"wrong payload type", []*commonpb.Log{wrongPayload}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t, backendReturningLogs(t, tc.logs))

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodPost, "/mirror-ledger/promote", nil, map[string]string{
				"ledgerName": "mirror-ledger",
			})

			require.Panics(t, func() {
				srv.handlePromoteLedger(w, r)
			})
		})
	}
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
