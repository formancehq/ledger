package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestWriteResponse_SingleTransaction(t *testing.T) {
	t.Parallel()

	for _, query := range []string{"", "?skipResponse=false", "?skipResponse=true"} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			entry := compactTransactionLog()
			original := proto.Clone(entry)
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
					require.False(t, req.GetSkipResponse(), "full logs are needed on local and forwarded paths")
					require.True(t, proto.Equal(servicepb.UnsignedApplyRequest("same-key", &servicepb.Request{
						Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
							Ledger: "l1", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
								CreateTransaction: &servicepb.CreateTransactionPayload{},
							}},
						}},
					}), req))

					return &domain.ApplyResult{Logs: []*commonpb.Log{entry}}, nil
				})
			handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
			r := httptest.NewRequest(http.MethodPost, "/v3/l1/transactions"+query, strings.NewReader(`{}`))
			r.Header.Set("Idempotency-Key", "same-key")
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
			if query == "?skipResponse=true" {
				require.Equal(t, `{"data":{"sequence":18446744073709551615}}`, strings.TrimSpace(w.Body.String()))
			} else {
				require.Contains(t, w.Body.String(), `"id":7`)
				require.NotContains(t, w.Body.String(), `"sequence"`)
			}
			require.True(t, proto.Equal(original, entry), "presentation must not mutate source logs")
		})
	}
}

func TestWriteResponse_OtherMutations(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		method, path, specPath, body, fullField string
		payload                                 *commonpb.LogPayload
	}{
		{http.MethodPost, "/v3/l1", "/v3/{ledgerName}", `{}`, `"name":"l1"`,
			&commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "l1"}}}},
		{http.MethodPost, "/v3/l1/promote", "/v3/{ledgerName}/promote", ``, `"name":"l1"`,
			&commonpb.LogPayload{Type: &commonpb.LogPayload_PromoteLedger{PromoteLedger: &commonpb.PromotedLedgerLog{Name: "l1"}}}},
		{http.MethodPut, "/v3/l1/numscripts/script", "/v3/{ledgerName}/numscripts/{name}", `{"content":"send","version":"v1"}`, `"name":"script"`,
			&commonpb.LogPayload{Type: &commonpb.LogPayload_SavedNumscript{SavedNumscript: &commonpb.SavedNumscriptLog{Info: &commonpb.NumscriptInfo{Name: "script", Version: "v1"}}}}},
		{http.MethodPost, "/v3/_/events-sinks", "/v3/_/events-sinks", `{"name":"sink","http":{"endpoint":"https://example.com"}}`, `"name":"sink"`,
			&commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: &commonpb.SinkConfig{Name: "sink"}}}}},
		{http.MethodPost, "/v3/l1/indexes", "/v3/{ledgerName}/indexes", `{"id":"metadata:TARGET_TYPE_ACCOUNT:color"}`, `"id":"metadata:TARGET_TYPE_ACCOUNT:color"`,
			&commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{}},
			}}}}}},
		{http.MethodPost, "/v3/l1/transactions/7/revert", "/v3/{ledgerName}/transactions/{transactionId}/revert", `{}`, `"id":8`,
			&commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{RevertedTransactionId: 7, RevertTransaction: &commonpb.Transaction{Id: 8}}},
			}}}}}},
	} {
		t.Run(tc.path, func(t *testing.T) {
			t.Parallel()
			entry := &commonpb.Log{Sequence: 99, Payload: tc.payload}
			original := proto.Clone(entry)
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: []*commonpb.Log{entry}}, nil).Times(3)
			handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
			doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
			require.NoError(t, err)
			var defaultBody string
			for _, query := range []string{"", "?skipResponse=false", "?skipResponse=true"} {
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path+query, strings.NewReader(tc.body)))
				require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
				if query == "?skipResponse=true" {
					require.JSONEq(t, `{"data":{"sequence":99}}`, w.Body.String())
				} else {
					require.Contains(t, w.Body.String(), tc.fullField)
					if query == "" {
						defaultBody = w.Body.String()
					} else {
						require.Equal(t, defaultBody, w.Body.String())
					}
				}
				var body any
				require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
				response := doc.Paths.Value(tc.specPath).GetOperation(tc.method).Responses.Status(w.Code)
				require.NoError(t, response.Value.Content["application/json"].Schema.Value.VisitJSON(body))
				require.True(t, proto.Equal(original, entry))
			}
		})
	}
}

func TestWriteResponse_InvalidOption(t *testing.T) {
	t.Parallel()

	for _, route := range []struct{ method, path string }{
		{http.MethodPost, "/v3/l1"}, {http.MethodPost, "/v3/l1/promote"},
		{http.MethodPut, "/v3/l1/numscripts/script"}, {http.MethodPost, "/v3/_/events-sinks"},
		{http.MethodPost, "/v3/l1/indexes"}, {http.MethodPost, "/v3/l1/transactions"},
		{http.MethodPost, "/v3/l1/transactions/7/revert"}, {http.MethodPost, "/v3/l1/bulk"},
	} {
		t.Run(route.path, func(t *testing.T) {
			t.Parallel()
			handler := NewHandler(logging.Testing(), NewMockBackend(gomock.NewController(t)), internalauth.AuthConfig{}, version.Info{})
			for _, value := range []string{"", "TRUE", "1", "yes", "null", "false&skipResponse=true", "true&skipResponse=true", "%GG"} {
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, httptest.NewRequest(route.method, route.path+"?skipResponse="+value, strings.NewReader(`{}`)))
				require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				require.Equal(t, "INVALID_REQUEST", decodeResponse[ErrorResponse](t, w).ErrorCode)
			}
		})
	}
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/v3/l1/transactions"}, {http.MethodDelete, "/v3/l1"},
		{http.MethodPost, "/v3/l1/accounts/a/metadata"},
	} {
		handler := NewHandler(logging.Testing(), NewMockBackend(gomock.NewController(t)), internalauth.AuthConfig{}, version.Info{})
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(route.method, route.path+"?skipResponse=true", nil))
		require.Equal(t, http.StatusBadRequest, w.Code)
		require.Contains(t, w.Body.String(), "unknown query parameter")
	}
}

func TestWriteResponse_RejectsImpossibleAcknowledgements(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"/v3/l1/transactions", "/v3/l1/indexes", "/v3/l1/bulk?atomic=true"} {
		for _, bad := range []string{"nil", "missing", "zero-sequence", "missing-payload", "wrong-payload"} {
			t.Run(path+"/"+bad, func(t *testing.T) {
				t.Parallel()
				entry := compactTransactionLog()
				var logs []*commonpb.Log
				switch bad {
				case "nil":
					logs = []*commonpb.Log{nil}
				case "missing":
				case "zero-sequence":
					entry.Sequence = 0
					logs = []*commonpb.Log{entry}
				case "missing-payload":
					entry.Payload = nil
					logs = []*commonpb.Log{entry}
				case "wrong-payload":
					entry.Payload = &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "l1"}}}
					logs = []*commonpb.Log{entry}
				}
				backend := NewMockBackend(gomock.NewController(t))
				backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{Logs: logs}, nil)
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				body := `{}`
				separator := "?"
				if strings.Contains(path, "bulk") {
					body = `[{"action":"CREATE_TRANSACTION","data":{}}]`
					separator = "&"
				} else if strings.Contains(path, "indexes") {
					body = `{"id":"metadata:TARGET_TYPE_ACCOUNT:color"}`
				}
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path+separator+"skipResponse=true", strings.NewReader(body)))
				require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
				require.NotContains(t, w.Body.String(), "signed-payload-canary")
			})
		}
	}
}

func TestWriteResponse_AuthenticationAndEmptyBulk(t *testing.T) {
	t.Parallel()
	for _, compact := range []string{"false", "true"} {
		for _, path := range []string{"/v3/l1/transactions", "/v3/l1/bulk"} {
			body := `{}`
			if strings.HasSuffix(path, "bulk") {
				body = `[{"action":"CREATE_TRANSACTION","data":{}}]`
			}
			handler := NewHandler(logging.Testing(), NewMockBackend(gomock.NewController(t)), internalauth.AuthConfig{Enabled: true}, version.Info{})
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path+"?skipResponse="+compact, strings.NewReader(body)))
			require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
		}
		handler := NewHandler(logging.Testing(), NewMockBackend(gomock.NewController(t)), internalauth.AuthConfig{}, version.Info{})
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v3/l1/bulk?atomic=true&skipResponse="+compact, strings.NewReader(`[]`)))
		require.Equal(t, http.StatusOK, w.Code)
		require.JSONEq(t, `{}`, w.Body.String())
	}
}

func compactTransactionLog() *commonpb.Log {
	return &commonpb.Log{Sequence: ^uint64(0), ResponseSignature: &signaturepb.SignedLog{Payload: []byte("signed-payload-canary")}, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
		Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Id: 3, Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{Id: 7},
			}},
		}}},
	}}}
}

func TestWriteResponse_Bulk(t *testing.T) {
	t.Parallel()

	for _, atomic := range []string{"false", "true"} {
		for _, compact := range []string{"false", "true"} {
			t.Run("atomic="+atomic+"/compact="+compact, func(t *testing.T) {
				t.Parallel()
				created := compactTransactionLog()
				created.Sequence = 101
				skipped := &commonpb.Log{Sequence: 102, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
					Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Id: 4, Data: &commonpb.LedgerLogPayload{
						Payload: &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{
							Details: commonpb.MustFailureDetails("TRANSACTION_REFERENCE_CONFLICT", map[string]string{"reference": "dup"}),
						}},
					}}},
				}}}
				originalCreated, originalSkipped := proto.Clone(created), proto.Clone(skipped)
				backend := NewMockBackend(gomock.NewController(t))
				calls := 0
				backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
						require.False(t, req.GetSkipResponse())
						if atomic == "true" {
							require.Len(t, req.GetUnsigned().GetRequests(), 2)
							require.Equal(t, "batch-key", req.GetUnsigned().GetIdempotencyKey())

							return &domain.ApplyResult{Logs: []*commonpb.Log{created, skipped}}, nil
						}
						require.Len(t, req.GetUnsigned().GetRequests(), 1)
						calls++
						if calls == 1 {
							require.Equal(t, "first", req.GetUnsigned().GetIdempotencyKey())

							return &domain.ApplyResult{Logs: []*commonpb.Log{created}}, nil
						}
						require.Equal(t, "second", req.GetUnsigned().GetIdempotencyKey())

						return &domain.ApplyResult{Logs: []*commonpb.Log{skipped}}, nil
					}).Times(map[bool]int{true: 1, false: 2}[atomic == "true"])
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				r := httptest.NewRequest(http.MethodPost, "/v3/l1/bulk?atomic="+atomic+"&skipResponse="+compact,
					strings.NewReader(`[{"action":"CREATE_TRANSACTION","ik":"first","data":{}},{"action":"CREATE_TRANSACTION","ik":"second","data":{"reference":"dup"},"skippableReasons":["TRANSACTION_REFERENCE_CONFLICT"]}]`))
				r.Header.Set("Idempotency-Key", "batch-key")
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())
				if compact == "true" {
					require.JSONEq(t, `{"data":[{"responseType":"CREATE_TRANSACTION","logID":3,"sequence":101},{"responseType":"CREATE_TRANSACTION","logID":4,"sequence":102,"data":{"skipped":true,"reason":"TRANSACTION_REFERENCE_CONFLICT","code":"TRANSACTION_REFERENCE_CONFLICT","facts":{"reference":"dup"}}}]}`, w.Body.String())
				} else {
					require.Contains(t, w.Body.String(), `"id":7`)
					require.Contains(t, w.Body.String(), `"skipped":true`)
					require.NotContains(t, w.Body.String(), `"sequence"`)
				}
				require.True(t, proto.Equal(originalCreated, created))
				require.True(t, proto.Equal(originalSkipped, skipped))
				requireBulkContract(t, "write-response-atomic-"+atomic+"-compact-"+compact, w)
			})
		}
	}
}
