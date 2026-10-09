package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/adapter/apierr"
	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func bulkContractSuccess() *commonpb.Log {
	return &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
		Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{
			Id: 17,
			Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
				CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 7}},
			}},
		}},
	}}}
}

// requireBulkContract validates actual router output, and optionally exports it
// for the generated SDK operation test. The SDK consumes these exact bytes,
// rather than a separately maintained approximation of the HTTP contract.
func requireBulkContract(t *testing.T, name string, w *httptest.ResponseRecorder) {
	t.Helper()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	response := doc.Paths.Value("/v3/{ledgerName}/bulk").Post.Responses.Status(w.Code)
	require.NotNil(t, response, "undeclared bulk status %d", w.Code)
	mediaType, _, err := mime.ParseMediaType(w.Header().Get("Content-Type"))
	require.NoError(t, err)
	var body any = w.Body.String()
	if mediaType == "application/json" {
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	}
	content := response.Value.Content[mediaType]
	require.NotNil(t, content, "undeclared bulk response media type %s", mediaType)
	require.NoError(t, content.Schema.Value.VisitJSON(body))
	if w.Code == http.StatusServiceUnavailable {
		require.Equal(t, "1", w.Header().Get("Retry-After"))
		require.Contains(t, response.Value.Headers, "Retry-After")
	}

	if dir := os.Getenv("LEDGER_BULK_SDK_FIXTURE_DIR"); dir != "" {
		require.NoError(t, os.MkdirAll(dir, 0o755))
		fixture, err := json.Marshal(map[string]any{
			"status": w.Code, "headers": w.Header(), "body": body,
		})
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dir, name+".json"), fixture, 0o600))
	}
}

func TestHandleBulk_ProcessingResponseContract(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		kind   domain.ErrorKind
		status int
	}{
		{"validation", domain.KindValidation, 400},
		{"not-found", domain.KindNotFound, 404},
		{"already-exists", domain.KindAlreadyExists, 409},
		{"conflict", domain.KindConflict, 409},
		{"precondition", domain.KindPrecondition, 400},
		{"unavailable", domain.KindUnavailable, 503},
		{"unauthenticated", domain.KindUnauthenticated, 401},
		{"permission-denied", domain.KindPermissionDenied, 403},
		{"internal", domain.KindInternal, 500},
		{"resource-exhausted", domain.KindResourceExhausted, 429},
	} {
		for _, continuation := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/continue=%t", tc.name, continuation), func(t *testing.T) {
				t.Parallel()
				backend := NewMockBackend(gomock.NewController(t))
				calls := 0
				backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, _ *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
						calls++
						if calls == 2 {
							// Decoded peer errors retain the sender's classification,
							// including reasons this build does not yet know.
							return nil, &apierr.Remote{KindValue: tc.kind, ReasonValue: "PEER_FAILURE", Msg: "peer failure"}
						}

						return &domain.ApplyResult{Logs: []*commonpb.Log{bulkContractSuccess()}}, nil
					}).Times(map[bool]int{false: 2, true: 3}[continuation])
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				body := "[" + strings.Trim(bulkWriteBody, "[]") + "," + strings.Trim(bulkWriteBody, "[]") + "," + strings.Trim(bulkWriteBody, "[]") + "]"
				r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v3/ledger1/bulk?continueOnFailure=%t", continuation), strings.NewReader(body))
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				wantStatus := tc.status
				if continuation && tc.status < 429 {
					wantStatus = 200
				}
				require.Equal(t, wantStatus, w.Code)
				response := decodeResponse[bulkResponse](t, w)
				require.Len(t, response.Data, 3)
				require.Equal(t, "CREATE_TRANSACTION", response.Data[0].ResponseType)
				require.Equal(t, uint64(17), response.Data[0].LogID)
				require.NotNil(t, response.Data[0].Data)
				require.Equal(t, "ERROR", response.Data[1].ResponseType)
				require.Equal(t, "PEER_FAILURE", response.Data[1].ErrorCode)
				require.Equal(t, "peer failure", response.Data[1].ErrorDescription)
				if continuation {
					require.Equal(t, "CREATE_TRANSACTION", response.Data[2].ResponseType)
				} else {
					require.Equal(t, "ERROR", response.Data[2].ResponseType)
					require.Equal(t, "context canceled", response.Data[2].ErrorDescription)
				}
				requireBulkContract(t, fmt.Sprintf("%s-continue-%t", tc.name, continuation), w)
			})
		}
	}
}

func TestHandleBulk_IdempotencyContract(t *testing.T) {
	t.Parallel()
	for _, atomic := range []bool{false, true} {
		for _, header := range []string{"batch-key", ""} {
			t.Run(fmt.Sprintf("atomic=%t/header=%s", atomic, header), func(t *testing.T) {
				t.Parallel()
				backend := NewMockBackend(gomock.NewController(t))
				var keys []string
				var batchSizes []int
				backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
						envelope := request.GetUnsigned()
						keys = append(keys, envelope.GetIdempotencyKey())
						batchSizes = append(batchSizes, len(envelope.GetRequests()))
						logs := make([]*commonpb.Log, len(envelope.GetRequests()))
						for i, action := range envelope.GetRequests() {
							require.Equal(t, "ledger1", action.GetApply().GetLedger())
							logs[i] = bulkContractSuccess()
						}

						return &domain.ApplyResult{Logs: logs}, nil
					}).AnyTimes()
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				body := `[{"action":"CREATE_TRANSACTION","ik":"element-one","data":{}},{"action":"CREATE_TRANSACTION","ik":"element-two","data":{}}]`
				r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v3/ledger1/bulk?atomic=%t", atomic), strings.NewReader(body))
				if header != "" {
					r.Header.Set("Idempotency-Key", header)
				}
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				require.Equal(t, http.StatusOK, w.Code)
				require.Len(t, decodeResponse[bulkResponse](t, w).Data, 2)
				if atomic {
					require.Equal(t, []string{header}, keys)
					require.Equal(t, []int{2}, batchSizes)
				} else {
					require.Equal(t, []string{"element-one", "element-two"}, keys)
					require.Equal(t, []int{1, 1}, batchSizes)
				}
			})
		}
	}
}

func TestHandleBulk_InfrastructureResponseContract(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"no-leader", commonpb.ErrNoLeader, http.StatusServiceUnavailable},
		{"cache-horizon", plan.ErrCacheHorizonExceeded, http.StatusServiceUnavailable},
		{"unknown-error", errors.New("private storage failure"), http.StatusInternalServerError},
	} {
		for _, continuation := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/continue=%t", tc.name, continuation), func(t *testing.T) {
				t.Parallel()
				backend := NewMockBackend(gomock.NewController(t))
				backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, tc.err).Times(1)
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v3/ledger1/bulk?continueOnFailure=%t", continuation), strings.NewReader(bulkWriteBody))
				r.Header.Set("X-Request-ID", "bulk-contract")
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				require.Equal(t, tc.status, w.Code)
				response := decodeResponse[bulkResponse](t, w)
				require.Len(t, response.Data, 1)
				require.Equal(t, "ERROR", response.Data[0].ResponseType)
				require.Equal(t, "ERROR", response.Data[0].ErrorCode)
				if tc.status == http.StatusInternalServerError {
					require.Equal(t, "internal server error (correlation ID: bulk-contract)", response.Data[0].ErrorDescription)
				}
				requireBulkContract(t, fmt.Sprintf("%s-continue-%t", tc.name, continuation), w)
			})
		}
	}
}

func TestHandleBulk_AtomicErrorContract(t *testing.T) {
	t.Parallel()
	for _, continuation := range []bool{false, true} {
		t.Run(fmt.Sprintf("continue=%t", continuation), func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, &domain.ErrTransactionNotFound{TransactionID: 42}).Times(1)
			handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
			body := `[{"action":"CREATE_TRANSACTION","data":{}},{"action":"REVERT_TRANSACTION","data":{"id":42}}]`
			r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/v3/ledger1/bulk?atomic=true&continueOnFailure=%t", continuation), strings.NewReader(body))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			wantStatus := http.StatusNotFound
			if continuation {
				wantStatus = http.StatusOK
			}
			require.Equal(t, wantStatus, w.Code)
			response := decodeResponse[bulkResponse](t, w)
			require.Len(t, response.Data, 2)
			for _, result := range response.Data {
				require.Equal(t, "ERROR", result.ResponseType)
				require.Equal(t, "TRANSACTION_NOT_FOUND", result.ErrorCode)
				require.Zero(t, result.LogID)
				require.Nil(t, result.Data)
			}
			requireBulkContract(t, fmt.Sprintf("atomic-continue-%t", continuation), w)
		})
	}
}

func TestHandleBulk_RequestErrorContract(t *testing.T) {
	t.Parallel()
	key, keySet := testKeyPair(t)
	for _, tc := range []struct {
		name   string
		body   string
		token  string
		auth   bool
		panic  bool
		status int
		code   string
	}{
		{"malformed", "not json", "", false, false, 400, "VALIDATION"},
		{"body-byte-limit", strings.Repeat(" ", int(defaultMaxBodySize)+1), "", false, false, 400, "VALIDATION"},
		{"element-count-limit", "[" + strings.Repeat(strings.Trim(bulkWriteBody, "[]")+",", defaultBulkMaxSize) + strings.Trim(bulkWriteBody, "[]") + "]", "", false, false, 413, "BULK_SIZE_EXCEEDED"},
		{"no-token", bulkWriteBody, "", true, false, 401, "UNAUTHENTICATED"},
		{"invalid-token", bulkWriteBody, "invalid", true, false, 401, "UNAUTHENTICATED"},
		{"insufficient-scope", bulkWriteBody, signToken(t, key, testClaims("ledger:read")), true, false, 403, "PERMISSION_DENIED"},
		{"panic-recovery", bulkWriteBody, "", false, true, 500, "INTERNAL_ERROR"},
		{"empty-success", "[]", "", false, false, 200, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			if tc.panic {
				backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
						panic("bulk contract panic")
					}).Times(1)
			}
			cfg := internalauth.AuthConfig{Enabled: tc.auth, KeySet: keySet, Issuer: testAuthIssuer, Service: "ledger", ScopeMapping: internalauth.DefaultMapping("ledger")}
			handler := NewHandler(logging.Testing(), backend, cfg, version.Info{})
			r := httptest.NewRequest(http.MethodPost, "/v3/ledger1/bulk?continueOnFailure=true", strings.NewReader(tc.body))
			r.Header.Set("X-Request-ID", "bulk-contract")
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code)
			if tc.name == "invalid-token" {
				require.Equal(t, "text/plain; charset=utf-8", w.Header().Get("Content-Type"))
				require.True(t, strings.HasPrefix(w.Body.String(), "invalid token:"))
				requireBulkContract(t, tc.name, w)

				return
			}
			response := decodeResponse[bulkResponse](t, w)
			require.Empty(t, response.Data)
			require.Equal(t, tc.code, response.ErrorCode)
			if tc.status != http.StatusOK {
				require.NotEmpty(t, response.ErrorMessage)
			}
			requireBulkContract(t, tc.name, w)
		})
	}
}
