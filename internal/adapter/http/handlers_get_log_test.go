package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetLog_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLog(gomock.Any(), uint64(7)).DoAndReturn(
		func(_ context.Context, _ uint64) (*commonpb.Log, error) {
			return &commonpb.Log{
				Sequence: 7,
				Payload: &commonpb.LogPayload{
					Type: &commonpb.LogPayload_Apply{
						Apply: &commonpb.ApplyLedgerLog{
							LedgerName: "ledger1",
							Log: &commonpb.LedgerLog{
								Data: &commonpb.LedgerLogPayload{
									Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
										CreatedTransaction: &commonpb.CreatedTransaction{
											Transaction: &commonpb.Transaction{Id: 1},
										},
									},
								},
							},
						},
					},
				},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/7", nil, map[string]string{
		"sequence": "7",
	})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// The swap to writeOKChecked routes the log through Log.MarshalJSON, which
	// emits the synthetic `type` discriminator LedgerLog.MarshalJSON injects.
	// protojson cannot emit it at all (it is not a proto field), so before this
	// change the body carried no discriminator and disagreed with the logs-list
	// route, which has always used sonic for the same commonpb.Log type.
	//
	// This does NOT yet round-trip: LedgerLogPayload.MarshalJSON wraps the
	// payload in its oneof field-name key ({"createdTransaction":{...}}) while
	// HydrateLog unmarshals `data` straight into the bare inner type, so
	// LedgerLog.UnmarshalJSON cannot rehydrate it. That asymmetry is a separate
	// pre-existing defect affecting the logs-list route identically, tracked on
	// its own ticket — deliberately not fixed here, because the encode side is
	// also the events-sink wire.
	body := w.Body.String()
	require.Contains(t, body, `"type":"NEW_TRANSACTION"`)
	require.Contains(t, body, `"sequence":7`)
	require.NotContains(t, body, `"sequence":"7"`)
	require.Contains(t, body, `"createdTransaction":`)
	require.NotContains(t, body, `"id":"1"`)
}

// TestHandleGetLog_SerializesThroughMarshalJSON pins the property EN-1622 is
// about: the single-log route must serialize its commonpb.Log through
// Log.MarshalJSON, matching the shape the logs-list route also produces. It
// diverged because get-log used protojson while the list used sonic, so the
// same type had two wire shapes depending on which route you asked.
func TestHandleGetLog_SerializesThroughMarshalJSON(t *testing.T) {
	t.Parallel()

	logValue := func() *commonpb.Log {
		return &commonpb.Log{
			Sequence: 7,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{
						LedgerName: "ledger1",
						Log: &commonpb.LedgerLog{
							Data: &commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
									CreatedTransaction: &commonpb.CreatedTransaction{
										Transaction: &commonpb.Transaction{Id: 1},
									},
								},
							},
						},
					},
				},
			},
		}
	}

	direct, err := logValue().MarshalJSON()
	require.NoError(t, err)

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLog(gomock.Any(), uint64(7)).DoAndReturn(
		func(_ context.Context, _ uint64) (*commonpb.Log, error) {
			return logValue(), nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/7", nil, map[string]string{"sequence": "7"})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.JSONEq(t, `{"data":`+string(direct)+`}`, w.Body.String())
}

func TestHandleGetLog_InvalidSequence(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/abc", nil, map[string]string{
		"sequence": "abc",
	})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetLog_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLog(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint64) (*commonpb.Log, error) {
			return nil, commonpb.NewNotFoundError("log %d not found", 9999)
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/9999", nil, map[string]string{
		"sequence": "9999",
	})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}

// TestHandleGetLog_StringAmountWire pins both amount wires of the single-log
// route (EN-1779).
//
// This route writes through writeOKChecked, whose sonic ConfigDefault leaves `<`
// and `&` unescaped and appends no trailing newline. The logs-list route serves
// the very same fixture (bigAmountLog) through ConfigStd, so its pinned bytes in
// TestHandleListLedgerLogs_StringAmountWire are deliberately different. The two
// log routes are NOT unified: each keeps the encoder it already had, because the
// opt-in header changes the amount format and nothing else. The encoder
// assertions below fail if either route drifts onto the other's writer.
//
// Amount assertions read the raw body bytes and never decode: the repository's
// sonic wrapper has no UseNumber, so decoding silently truncates 2^53+1.
func TestHandleGetLog_StringAmountWire(t *testing.T) {
	t.Parallel()

	const (
		defaultBody = `{"data":{"sequence":7,"payload":{"apply":{"ledgerName":"ledger1","log":{"type":"NEW_TRANSACTION","data":{"createdTransaction":{"transaction":{"postings":[{"source":"world<&>","destination":"alice&<bob","amount":9007199254740993,"asset":"USD/2","color":""}],"metadata":{},"id":7,"reverted":false}}},"id":7}}},"responseSignature":{}}}`
		optInBody   = `{"data":{"sequence":7,"payload":{"apply":{"ledgerName":"ledger1","log":{"type":"NEW_TRANSACTION","data":{"createdTransaction":{"transaction":{"postings":[{"source":"world<&>","destination":"alice&<bob","amount":"9007199254740993","asset":"USD/2","color":""}],"metadata":{},"id":7,"reverted":false}}},"id":7}}},"responseSignature":{}}}`
	)

	requireOnlyAmountQuotingDiffers(t, defaultBody, optInBody)

	type testCase struct {
		name        string
		headerValue string // empty means the header is not sent at all
		wantBody    string
		wantAmount  string // the amount rendering that must appear
		notAmount   string // the other mode's rendering, which must not
	}

	testCases := []testCase{
		{
			name:       "default wire keeps the bare number",
			wantBody:   defaultBody,
			wantAmount: `"amount":` + aboveJSNumberLimit,
			notAmount:  `"amount":"` + aboveJSNumberLimit + `"`,
		},
		{
			name:        "opt-in wire quotes the decimal",
			headerValue: "true",
			wantBody:    optInBody,
			wantAmount:  `"amount":"` + aboveJSNumberLimit + `"`,
			notAmount:   `"amount":` + aboveJSNumberLimit,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().GetLog(gomock.Any(), uint64(7)).DoAndReturn(
				func(_ context.Context, _ uint64) (*commonpb.Log, error) {
					return bigAmountLog(t, 7), nil
				}).AnyTimes()
			srv := newTestServer(t, backend)

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodGet, "/logs/7", nil, map[string]string{"sequence": "7"})

			if tc.headerValue != "" {
				r.Header.Set("Formance-Bigint-As-String", tc.headerValue)
			}

			srv.handleGetLog(w, r)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Equal(t, tc.wantBody, w.Body.String())
			require.Contains(t, w.Body.String(), tc.wantAmount)
			require.NotContains(t, w.Body.String(), tc.notAmount)

			// Encoder assertions, in both modes: this route must stay on
			// ConfigDefault. Without them a swap to the logs-list route's writer
			// would only show up as two edited literals.
			require.False(t, strings.HasSuffix(w.Body.String(), "\n"),
				"the single-log route writes through ConfigDefault, which appends no trailing newline")
			require.Contains(t, w.Body.String(), "<",
				"the single-log route writes through ConfigDefault, which does not HTML-escape")
		})
	}
}
