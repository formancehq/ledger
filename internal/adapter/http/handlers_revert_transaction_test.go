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

func TestHandleRevertTransaction_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
											RevertedTransaction: &commonpb.RevertedTransaction{
												RevertTransaction: &commonpb.Transaction{
													Id: 2,
												},
											},
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
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

// TestHandleRevertTransaction_LogContractViolations locks in the exact-one
// typed-log contract: exactly one non-nil Apply log carrying a
// RevertedTransaction inner payload. Any other cardinality, a nil sole log, a
// non-Apply outer payload, or a mismatched inner payload must fail loudly
// through assert.Unreachable and a panic (the jsonRecoverer turns the panic
// into a sanitized 500 in production).
func TestHandleRevertTransaction_LogContractViolations(t *testing.T) {
	t.Parallel()

	reverted := &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
		Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
				RevertedTransaction: &commonpb.RevertedTransaction{RevertTransaction: &commonpb.Transaction{Id: 2}},
			},
		}}},
	}}}
	wrongOuter := &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "ledger1"}},
	}}
	wrongInner := &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
		Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 1}},
			},
		}}},
	}}}
	emptyBody := &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
		Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: nil},
		}}},
	}}}

	cases := []struct {
		name    string
		logs    []*commonpb.Log
		wantMsg string
	}{
		{"zero logs", []*commonpb.Log{}, "apply did not return exactly one log"},
		{"two logs", []*commonpb.Log{reverted, reverted}, "apply did not return exactly one log"},
		{"nil sole log", []*commonpb.Log{nil}, "apply returned a nil log"},
		{"wrong outer payload", []*commonpb.Log{wrongOuter}, "apply returned an unexpected log payload type"},
		{"wrong inner payload", []*commonpb.Log{wrongInner}, "apply returned an unexpected log payload type"},
		{"empty payload body", []*commonpb.Log{emptyBody}, "apply returned a log with no payload body"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t, backendReturningLogs(t, tc.logs))

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", nil, map[string]string{
				"ledgerName":    "ledger1",
				"transactionId": "1",
			})

			requirePanicsContaining(t, tc.wantMsg, func() {
				srv.handleRevertTransaction(w, r)
			})
		})
	}
}

func TestHandleRevertTransaction_AlreadyReverted(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, &domain.ErrTransactionAlreadyReverted{TransactionID: 1}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusConflict, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "TRANSACTION_ALREADY_REVERTED", resp.ErrorCode)
}

func TestHandleRevertTransaction_InvalidTxID(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/abc/revert", nil, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "abc",
	})

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRevertTransaction_WithBody(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
											RevertedTransaction: &commonpb.RevertedTransaction{
												RevertTransaction: &commonpb.Transaction{
													Id: 2,
												},
											},
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
	body := strings.NewReader(`{"force": true, "atEffectiveDate": true}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.Header.Set("Content-Length", "42")

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

// revertPayloadFromApply extracts the RevertTransactionPayload that the handler
// forwarded to the backend, so tests can assert the metadata mapping.
func revertPayloadFromApply(t *testing.T, req *servicepb.ApplyRequest) *servicepb.RevertTransactionPayload {
	t.Helper()

	requests := req.GetUnsigned().GetRequests()
	require.Len(t, requests, 1)

	action := requests[0].GetApply().GetAction()
	rt, ok := action.GetData().(*servicepb.LedgerAction_RevertTransaction)
	require.True(t, ok, "expected a revert-transaction action")

	return rt.RevertTransaction
}

func revertBackendReturningLog(t *testing.T, captured **servicepb.RevertTransactionPayload) *MockBackend {
	t.Helper()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			if captured != nil {
				*captured = revertPayloadFromApply(t, req)
			}

			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
											RevertedTransaction: &commonpb.RevertedTransaction{
												RevertTransaction: &commonpb.Transaction{Id: 2},
											},
										},
									},
								},
							},
						},
					},
				},
			}, nil
		}).AnyTimes()

	return backend
}

// TestHandleRevertTransaction_TypedMetadata verifies that typed metadata values
// (string, numeric, boolean, and date-as-string) survive the HTTP revert path
// without being coerced or dropped (EN-1509).
func TestHandleRevertTransaction_TypedMetadata(t *testing.T) {
	t.Parallel()

	var captured *servicepb.RevertTransactionPayload
	backend := revertBackendReturningLog(t, &captured)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"metadata":{"reason":"fraud","count":42,"negative":-7,"active":true,"effectiveAt":"2026-07-11T00:00:00Z"}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.ContentLength = int64(body.Len())

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, captured)

	got := commonpb.MetadataToAnyMap(captured.GetMetadata())
	require.Equal(t, "fraud", got["reason"])
	require.EqualValues(t, uint64(42), got["count"])
	require.EqualValues(t, int64(-7), got["negative"])
	require.Equal(t, true, got["active"])
	require.Equal(t, "2026-07-11T00:00:00Z", got["effectiveAt"])
}

// TestHandleRevertTransaction_StringMetadata keeps the plain string-metadata
// contract green alongside the typed one.
func TestHandleRevertTransaction_StringMetadata(t *testing.T) {
	t.Parallel()

	var captured *servicepb.RevertTransactionPayload
	backend := revertBackendReturningLog(t, &captured)
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"metadata":{"reason":"duplicate"}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.ContentLength = int64(body.Len())

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
	require.NotNil(t, captured)

	got := commonpb.MetadataToAnyMap(captured.GetMetadata())
	require.Equal(t, "duplicate", got["reason"])
}

// TestHandleRevertTransaction_InvalidMetadata verifies unsupported metadata
// values (objects/arrays) are rejected with 400 INVALID_REQUEST instead of
// being silently dropped (EN-1509).
func TestHandleRevertTransaction_InvalidMetadata(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"metadata":{"nested":{"not":"allowed"}}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", body, map[string]string{
		"ledgerName":    "ledger1",
		"transactionId": "1",
	})
	r.ContentLength = int64(body.Len())

	srv.handleRevertTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INVALID_REQUEST", resp.ErrorCode)
}

// TestHandleRevertTransaction_StringAmountWire pins both amount wires of the
// revert route (EN-1779). Both pinned literals carry this route's ConfigStd
// framing — `<`/`&` HTML-escaped and a trailing newline — so giving the opt-in
// branch a ConfigDefault writer would fail here instead of quietly making the
// header change the escaping as well as the amount.
//
// Amount assertions read the raw body bytes and never decode: the repository's
// sonic wrapper has no UseNumber, so decoding silently truncates 2^53+1.
func TestHandleRevertTransaction_StringAmountWire(t *testing.T) {
	t.Parallel()

	const (
		defaultBody = `{"data":{"revertedTransactionId":1,"revertTransaction":{"postings":[{"source":"world\u003c\u0026\u003e","destination":"alice\u0026\u003cbob","amount":9007199254740993,"asset":"USD/2","color":""}],"metadata":{},"id":2,"reverted":false}}}` + "\n"
		optInBody   = `{"data":{"revertedTransactionId":1,"revertTransaction":{"postings":[{"source":"world\u003c\u0026\u003e","destination":"alice\u0026\u003cbob","amount":"9007199254740993","asset":"USD/2","color":""}],"metadata":{},"id":2,"reverted":false}}}` + "\n"
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

			srv := newTestServer(t, backendReturningLogs(t, []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
							RevertedTransaction: &commonpb.RevertedTransaction{
								RevertedTransactionId: 1,
								RevertTransaction:     bigAmountTransaction(t, 2),
							},
						},
					}}},
				}}},
			}))

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodPost, "/ledger1/transactions/1/revert", nil, map[string]string{
				"ledgerName":    "ledger1",
				"transactionId": "1",
			})

			if tc.headerValue != "" {
				r.Header.Set("Formance-Bigint-As-String", tc.headerValue)
			}

			srv.handleRevertTransaction(w, r)

			require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
			require.Equal(t, tc.wantBody, w.Body.String())
			require.Contains(t, w.Body.String(), tc.wantAmount)
			require.NotContains(t, w.Body.String(), tc.notAmount)
		})
	}
}
