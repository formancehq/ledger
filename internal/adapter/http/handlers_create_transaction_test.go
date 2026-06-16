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

func TestHandleCreateTransaction_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, requests ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{
					Payload: &commonpb.LogPayload{
						Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{
								Log: &commonpb.LedgerLog{
									Data: &commonpb.LedgerLogPayload{
										Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
											CreatedTransaction: &commonpb.CreatedTransaction{
												Transaction: &commonpb.Transaction{
													Id: 1,
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
	body := strings.NewReader(`{"script":{"plain":"send [USD 100] (\n  source = @world\n  destination = @users:001\n)"}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreateTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code)
}

func TestHandleCreateTransaction_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{invalid`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreateTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleCreateTransaction_InsufficientFunds(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return nil, &domain.ErrInsufficientFunds{
				Account: "users:001",
				Asset:   "USD",
				Amount:  "100",
				Balance: "50",
			}
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"script":{"plain":"send [USD 100] (\n  source = @users:001\n  destination = @users:002\n)"}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreateTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "INSUFFICIENT_FUNDS", resp.ErrorCode)
}

func TestHandleCreateTransaction_MissingLedgerName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{}`)
	r := newRequest(t, http.MethodPost, "/transactions", body, map[string]string{
		"ledgerName": "",
	})

	srv.handleCreateTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

// TestHandleCreateTransaction_CamelCaseFields exercises the multi-word JSON
// keys the REST contract advertises (scriptReference, accountMetadata,
// expandVolumes). Before #452 the protobuf-generated Go struct tags used
// snake_case, so a plain encoding/json decode silently dropped these
// fields and the FSM observed an empty payload. The hand-written
// CreateTransactionPayload.UnmarshalJSON must now route them to the
// backend intact.
func TestHandleCreateTransaction_CamelCaseFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		body   string
		verify func(t *testing.T, ct *servicepb.CreateTransactionPayload)
	}{
		{
			name: "scriptReference",
			body: `{"scriptReference":{"name":"payment","version":"1.0.0","vars":{"amt":"USD 5","src":"users:alice","dst":"users:bob"}}}`,
			verify: func(t *testing.T, ct *servicepb.CreateTransactionPayload) {
				t.Helper()
				ref := ct.GetScriptReference()
				require.NotNil(t, ref, "scriptReference must reach the backend")
				require.Equal(t, "payment", ref.GetName())
				require.Equal(t, "1.0.0", ref.GetVersion())
				require.Equal(t, map[string]string{
					"amt": "USD 5",
					"src": "users:alice",
					"dst": "users:bob",
				}, ref.GetVars())
			},
		},
		{
			name: "accountMetadata",
			body: `{"postings":[{"source":"world","destination":"users:alice","amount":1,"asset":"USD"}],"accountMetadata":{"users:alice":{"vip":"yes"}}}`,
			verify: func(t *testing.T, ct *servicepb.CreateTransactionPayload) {
				t.Helper()
				am := ct.GetAccountMetadata()
				require.Contains(t, am, "users:alice")
				require.Contains(t, am["users:alice"].GetValues(), "vip")
				require.Equal(t, "yes", am["users:alice"].GetValues()["vip"].GetStringValue())
			},
		},
		{
			name: "expandVolumes",
			body: `{"postings":[{"source":"world","destination":"users:alice","amount":1,"asset":"USD"}],"expandVolumes":true}`,
			verify: func(t *testing.T, ct *servicepb.CreateTransactionPayload) {
				t.Helper()
				require.True(t, ct.GetExpandVolumes())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var captured *servicepb.CreateTransactionPayload

			backend := &mockBackend{
				applyFn: func(_ context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
					captured = requests[0].GetApply().GetAction().GetCreateTransaction()

					return []*commonpb.Log{
						{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
							Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
								Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
									CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 1}},
								},
							}}},
						}}},
					}, nil
				},
			}
			srv := newTestServer(t, backend)

			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodPost, "/ledger1/transactions", strings.NewReader(tc.body), map[string]string{
				"ledgerName": "ledger1",
			})
			srv.handleCreateTransaction(w, r)

			require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
			require.NotNil(t, captured, "backend must observe the payload")
			tc.verify(t, captured)
		})
	}
}

// TestHandleCreateTransaction_PostingsAndScriptConflict ensures that the
// REST handler rounds-trips the validation sentinel raised by the new
// structural gate when a caller combines explicit postings with a script
// reference (or inline script). Decoding the camelCase body is what
// surfaces the conflict in the first place — before #452 the
// scriptReference key was silently dropped, hiding the violation.
func TestHandleCreateTransaction_PostingsAndScriptConflict(t *testing.T) {
	t.Parallel()

	var captured *servicepb.CreateTransactionPayload

	backend := &mockBackend{
		applyFn: func(_ context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
			captured = requests[0].GetApply().GetAction().GetCreateTransaction()

			return nil, &domain.BusinessError{Err: domain.ErrPostingsAndScriptConflict}
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"postings":[{"source":"world","destination":"users:alice","amount":1,"asset":"USD"}],"scriptReference":{"name":"payment","version":"1.0.0"}}`)
	r := newRequest(t, http.MethodPost, "/ledger1/transactions", body, map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreateTransaction(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	resp := decodeResponse[ErrorResponse](t, w)
	require.Equal(t, "VALIDATION", resp.ErrorCode)
	require.NotNil(t, captured)
	require.Len(t, captured.GetPostings(), 1, "postings must reach the backend so the gate can see the conflict")
	require.NotNil(t, captured.GetScriptReference())
}

// TestHandleCreateTransaction_UnknownFieldsAreLenient confirms that
// clients are allowed to send extra keys without getting a 400. The
// hand-written UnmarshalJSON keeps the lenient behavior of the previous
// encoding/json route.
func TestHandleCreateTransaction_UnknownFieldsAreLenient(t *testing.T) {
	t.Parallel()

	backend := &mockBackend{
		applyFn: func(_ context.Context, _ ...*servicepb.Request) ([]*commonpb.Log, error) {
			return []*commonpb.Log{
				{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
							CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 1}},
						},
					}}},
				}}},
			}, nil
		},
	}
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := `{"script":{"plain":"send [USD 1] (source = @world destination = @a)"},"unknownField":"ignored"}`
	r := newRequest(t, http.MethodPost, "/ledger1/transactions", strings.NewReader(body), map[string]string{
		"ledgerName": "ledger1",
	})

	srv.handleCreateTransaction(w, r)

	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
}
