package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// bulkWriteBody is a single-element bulk payload whose action (CREATE_TRANSACTION)
// requires the ledger:TransactionWrite granular scope.
const bulkWriteBody = `[{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD/2"}]}}]`

func TestHandleBulk_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`not json`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleBulk_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`[]`)
	r := newRequest(t, http.MethodPost, "/bulk", body, map[string]string{
		"ledgerName": "",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleBulk_SizeLimitExceeded(t *testing.T) {
	t.Parallel()

	srv := newTestServerWithBulkLimit(t, NewMockBackend(gomock.NewController(t)), 1)

	// Two elements but limit is 1
	w := httptest.NewRecorder()
	body := strings.NewReader(`[
		{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD/2"}]}},
		{"action":"CREATE_TRANSACTION","data":{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD/2"}]}}
	]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
}

// TestHandleBulk_OrderSkippedSurfacesInResponse pins BOTH ends of the
// per-entry skip wiring for the bulk endpoint:
//
//   - JSON decode side: a top-level `skippableReasons` on the entry (NOT
//     nested inside data) is decoded onto BulkElement.SkippableReasons and
//     hoisted onto the LedgerApplyRequest the backend receives. The test
//     captures the request and asserts the list arrives untruncated.
//   - Response side: when the FSM matched a whitelisted business failure
//     and returned an OrderSkipped log, the bulk result's `data` carries
//     a structured OrderSkippedResponse (skipped/reason/context) — not
//     the legacy null that dropped the correlator.
func TestHandleBulk_OrderSkippedSurfacesInResponse(t *testing.T) {
	t.Parallel()

	var received *servicepb.ApplyRequest

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			received = req

			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Id: 17,
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_OrderSkipped{
											OrderSkipped: &commonpb.OrderSkippedLog{
												Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
												Context: map[string]string{
													"reference":             "dup",
													"existingTransactionId": "42",
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

	body := strings.NewReader(`[
		{"action":"CREATE_TRANSACTION","data":{"reference":"dup"},"skippableReasons":["TRANSACTION_REFERENCE_CONFLICT"]}
	]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})
	w := httptest.NewRecorder()

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// Request-side assertion: the per-entry opt-in must reach the backend
	// on the LedgerApplyRequest envelope (NOT on CreateTransactionPayload).
	require.NotNil(t, received, "backend must have been called")
	require.Len(t, received.GetUnsigned().GetRequests(), 1)
	require.Equal(t,
		[]commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
		received.GetUnsigned().GetRequests()[0].GetApply().GetSkippableReasons(),
	)

	resp := decodeResponse[bulkResponse](t, w)
	require.Len(t, resp.Data, 1)
	require.Equal(t, "CREATE_TRANSACTION", resp.Data[0].ResponseType)
	require.Equal(t, uint64(17), resp.Data[0].LogID)
	require.NotNil(t, resp.Data[0].Data)

	// Data is unmarshalled as a map[string]any (interface{} round-trip).
	skip, ok := resp.Data[0].Data.(map[string]any)
	require.True(t, ok, "Data must be the structured OrderSkippedResponse shape (got %T)", resp.Data[0].Data)
	require.Equal(t, true, skip["skipped"])
	require.Equal(t, "TRANSACTION_REFERENCE_CONFLICT", skip["reason"])

	// Reason-specific correlator must round-trip through the bulk writer
	// so clients can act on the existing tx id without a follow-up GET.
	ctx, ok := skip["context"].(map[string]any)
	require.True(t, ok, "context must round-trip as a nested object (got %T)", skip["context"])
	require.Equal(t, "dup", ctx["reference"])
	require.Equal(t, "42", ctx["existingTransactionId"])
}

func TestHandleBulk_EmptyArray(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`[]`)
	r := newRequest(t, http.MethodPost, "/ledger1/bulk", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleBulk(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestRunBulkAtomic_AllFail(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("atomic failure")
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return nil, expectedErr
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}}
	results := srv.runBulkAtomic(context.Background(), "", requests)

	require.Len(t, results, 2)

	for _, r := range results {
		require.ErrorIs(t, r.err, expectedErr)
	}
}

func TestRunBulkAtomic_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Id: 1}}}}},
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Id: 2}}}}},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}}
	results := srv.runBulkAtomic(context.Background(), "", requests)

	require.Len(t, results, 2)

	for _, r := range results {
		require.NoError(t, r.err)
		require.NotNil(t, r.log)
	}
}

func TestRunBulkSequential_StopOnError(t *testing.T) {
	t.Parallel()

	callCount := 0
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			callCount++
			if callCount == 1 {
				return nil, errors.New("first fails")
			}

			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{}}}}},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}, {}}
	keys := []string{"", "", ""}
	results := srv.runBulkSequential(context.Background(), requests, keys, false)

	require.Len(t, results, 3)
	require.Error(t, results[0].err)
	require.ErrorIs(t, results[1].err, context.Canceled)
	require.ErrorIs(t, results[2].err, context.Canceled)
}

func TestRunBulkSequential_ContinueOnFailure(t *testing.T) {
	t.Parallel()

	callCount := 0
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			callCount++
			if callCount == 1 {
				return nil, errors.New("first fails")
			}

			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{}}}}},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	requests := []*servicepb.Request{{}, {}}
	keys := []string{"", ""}
	results := srv.runBulkSequential(context.Background(), requests, keys, true)

	require.Len(t, results, 2)
	require.Error(t, results[0].err)
	require.NoError(t, results[1].err)
}

// TestHandleBulk_AuthEnabled_NoToken_Unauthorized covers the unauthenticated
// write path: auth is enabled with the default mapping (no anonymous scopes),
// the request carries no bearer token, and a write element must be rejected with
// 401 before reaching the backend. The mock backend has no Apply expectation, so
// any call to it fails the test.
func TestHandleBulk_AuthEnabled_NoToken_Unauthorized(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	authCfg := internalauth.AuthConfig{
		Enabled:      true,
		ScopeMapping: internalauth.DefaultMapping("ledger"),
	}
	handler := NewHandler(logging.Testing(), backend, authCfg, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v3/ledger1/bulk", strings.NewReader(bulkWriteBody))

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusUnauthorized, w.Code)
	require.Equal(t, "UNAUTHENTICATED", decodeResponse[bulkResponse](t, w).ErrorCode)
}

// TestHandleBulk_AuthDisabled_NoToken_Allowed is the control for the case above:
// with auth disabled the same no-token write must pass through to the backend.
func TestHandleBulk_AuthDisabled_NoToken_Allowed(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{}}}}},
			}, nil
		}).Times(1)
	handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v3/ledger1/bulk", strings.NewReader(bulkWriteBody))

	handler.ServeHTTP(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

// TestHandleBulk_StringAmountWire pins the bulk route's two amount wires
// (EN-1779), one case per payload arm the per-element `data` field can carry.
//
// Two of the three arms must be byte-identical in both modes, and that is the
// point of the table: the mode is applied inside the CreatedTransaction arm, not
// at the write site, so an OrderSkipped element (which carries no amount) and a
// CreatedTransaction whose inner transaction is nil both come out unchanged. The
// nil case also measures the `omitempty` question on bulkAPIResult.Data: a typed
// nil pointer behind an `any` is a non-nil interface, so the field is NOT
// dropped and already renders `"data":null` today — exactly what the wrapper's
// nil-inner-pointer guard emits.
//
// Bulk writes through writeCheckedBody (ConfigStd), so `<`/`&` come out escaped
// and the body ends in a newline, in both modes.
//
// Amount assertions read the raw body bytes and never decode: the repository's
// sonic wrapper has no UseNumber, so decoding silently truncates 2^53+1.
func TestHandleBulk_StringAmountWire(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name    string
		payload *commonpb.LedgerLogPayload
		// amountMoves marks the only arm whose rendering the header may change.
		// Every other arm asserts the two bodies are equal instead.
		amountMoves   bool
		htmlSensitive bool
		defaultBody   string
		optInBody     string
	}

	testCases := []testCase{
		{
			name: "created transaction carries the amount",
			payload: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{
						Transaction: bigAmountTransaction(t, 1),
					},
				},
			},
			amountMoves:   true,
			htmlSensitive: true,
			defaultBody:   `{"data":[{"data":{"postings":[{"source":"world\u003c\u0026\u003e","destination":"alice\u0026\u003cbob","amount":9007199254740993,"asset":"USD/2","color":""}],"metadata":{},"id":1,"reverted":false},"responseType":"CREATE_TRANSACTION","logID":17}]}` + "\n",
			optInBody:     `{"data":[{"data":{"postings":[{"source":"world\u003c\u0026\u003e","destination":"alice\u0026\u003cbob","amount":"9007199254740993","asset":"USD/2","color":""}],"metadata":{},"id":1,"reverted":false},"responseType":"CREATE_TRANSACTION","logID":17}]}` + "\n",
		},
		{
			name: "order skipped carries no amount",
			payload: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_OrderSkipped{
					OrderSkipped: &commonpb.OrderSkippedLog{
						Reason:  commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
						Context: map[string]string{"reference": "dup<&>"},
					},
				},
			},
			htmlSensitive: true,
			defaultBody:   `{"data":[{"data":{"skipped":true,"reason":"TRANSACTION_REFERENCE_CONFLICT","context":{"reference":"dup\u003c\u0026\u003e"}},"responseType":"CREATE_TRANSACTION","logID":17}]}` + "\n",
			optInBody:     `{"data":[{"data":{"skipped":true,"reason":"TRANSACTION_REFERENCE_CONFLICT","context":{"reference":"dup\u003c\u0026\u003e"}},"responseType":"CREATE_TRANSACTION","logID":17}]}` + "\n",
		},
		{
			name: "created transaction with a nil inner transaction",
			payload: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{},
				},
			},
			defaultBody: `{"data":[{"data":null,"responseType":"CREATE_TRANSACTION","logID":17}]}` + "\n",
			optInBody:   `{"data":[{"data":null,"responseType":"CREATE_TRANSACTION","logID":17}]}` + "\n",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.amountMoves {
				requireOnlyAmountQuotingDiffers(t, tc.defaultBody, tc.optInBody)
			} else {
				require.Equal(t, tc.defaultBody, tc.optInBody,
					"an arm that carries no amount must be byte-identical in both modes")
			}

			modes := []struct {
				name        string
				headerValue string // empty means the header is not sent at all
				wantBody    string
			}{
				{name: "default wire", wantBody: tc.defaultBody},
				{name: "opt-in wire", headerValue: "true", wantBody: tc.optInBody},
			}

			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					t.Parallel()

					backend := NewMockBackend(gomock.NewController(t))
					backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
						func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
							return []*commonpb.Log{{
								Payload: &commonpb.LogPayload{
									Type: &commonpb.LogPayload_Apply{
										Apply: &commonpb.ApplyLedgerLog{
											Log: &commonpb.LedgerLog{Id: 17, Data: tc.payload},
										},
									},
								},
							}}, nil
						}).Times(1)
					srv := newTestServer(t, backend)

					w := httptest.NewRecorder()
					r := newRequest(t, http.MethodPost, "/ledger1/bulk",
						strings.NewReader(bulkWriteBody), map[string]string{"ledgerName": "ledger1"})

					if mode.headerValue != "" {
						r.Header.Set("Formance-Bigint-As-String", mode.headerValue)
					}

					srv.handleBulk(w, r)

					require.Equal(t, http.StatusOK, w.Code, w.Body.String())
					require.Equal(t, mode.wantBody, w.Body.String())

					// Bulk stays on ConfigStd in both modes: writeCheckedBody is
					// byte-identical to the writeJSONResponse it replaced.
					require.True(t, strings.HasSuffix(w.Body.String(), "\n"),
						"bulk writes through ConfigStd, which appends a trailing newline")

					if tc.htmlSensitive {
						require.NotContains(t, w.Body.String(), "<",
							"bulk writes through ConfigStd, which HTML-escapes")
					}
				})
			}
		})
	}
}

// TestHandleBulk_StringAmountWireOnlyWrapsTheTransactionArm asserts the amount
// rendering is decided inside the CreatedTransaction arm rather than at the
// write site. Wrapping the whole bulkResponse would also reach the OrderSkipped
// arm, so this pins the negative: an opted-in response whose single element is a
// skip must be identical to the default one, and must never quote a number.
func TestHandleBulk_StringAmountWireOnlyWrapsTheTransactionArm(t *testing.T) {
	t.Parallel()

	bodies := make([]string, 0, 2)

	for _, headerValue := range []string{"", "true"} {
		backend := NewMockBackend(gomock.NewController(t))
		backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
			func(_ context.Context, _ *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
				return []*commonpb.Log{{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Id: 17,
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_OrderSkipped{
											OrderSkipped: &commonpb.OrderSkippedLog{
												Reason: commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
											},
										},
									},
								},
							},
						},
					},
				}}, nil
			}).Times(1)

		w := httptest.NewRecorder()
		r := newRequest(t, http.MethodPost, "/ledger1/bulk",
			strings.NewReader(bulkWriteBody), map[string]string{"ledgerName": "ledger1"})

		if headerValue != "" {
			r.Header.Set("Formance-Bigint-As-String", headerValue)
		}

		newTestServer(t, backend).handleBulk(w, r)

		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), `"logID":17`)
		require.NotContains(t, w.Body.String(), `"logID":"17"`,
			"the header quotes posting amounts, never other numbers")

		bodies = append(bodies, w.Body.String())
	}

	require.Equal(t, bodies[0], bodies[1],
		"a skip-only bulk response must not change when the opt-in header is sent")
}
