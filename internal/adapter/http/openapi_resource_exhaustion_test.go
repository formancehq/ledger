package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

// These registered write routes all enter Apply, where the admission disk
// gate and authoritative business-log allocator can reject the request.
// Mock only the transport's controller boundary; use the real HTTP router,
// input parsing, error mapping and OpenAPI response validation.
func TestOpenAPI_WriteResourceExhaustion(t *testing.T) {
	t.Parallel()
	doc, err := openapi3.NewLoader().LoadFromFile("../../../openapi.yml")
	require.NoError(t, err)
	operations := []struct{ method, path, body string }{
		{http.MethodPost, "/v3/{ledgerName}", `{}`},
		{http.MethodDelete, "/v3/{ledgerName}", ""},
		{http.MethodPost, "/v3/{ledgerName}/promote", ""},
		{http.MethodPut, "/v3/{ledgerName}/numscripts/{name}", `{"content":"send [USD 1] ( source = @world destination = @alice )","version":"1.0.0"}`},
		{http.MethodPost, "/v3/{ledgerName}/indexes", `{"id":"metadata:TARGET_TYPE_ACCOUNT:color"}`},
		{http.MethodDelete, "/v3/{ledgerName}/indexes/{canonicalId}", ""},
		{http.MethodPost, "/v3/{ledgerName}/transactions", `{"postings":[{"source":"world","destination":"alice","asset":"USD","amount":1}]}`},
		{http.MethodPost, "/v3/{ledgerName}/transactions/{transactionId}/revert", ""},
		{http.MethodPost, "/v3/{ledgerName}/transactions/{transactionId}/metadata", `{"approved":true}`},
		{http.MethodDelete, "/v3/{ledgerName}/transactions/{transactionId}/metadata/{key}", ""},
		{http.MethodPost, "/v3/{ledgerName}/accounts/{address}/metadata", `{"approved":true}`},
		{http.MethodDelete, "/v3/{ledgerName}/accounts/{address}/metadata/{key}", ""},
		{http.MethodPost, "/v3/{ledgerName}/metadata", `{"approved":true}`},
		{http.MethodDelete, "/v3/{ledgerName}/metadata/{key}", ""},
		{http.MethodPut, "/v3/{ledgerName}/metadata-schema/{targetType}/{key}", `{"type":"bool"}`},
		{http.MethodDelete, "/v3/{ledgerName}/metadata-schema/{targetType}/{key}", ""},
		{http.MethodPost, "/v3/{ledgerName}/account-types", `{"name":"customers","pattern":"customers:{id}"}`},
		{http.MethodDelete, "/v3/{ledgerName}/account-types/{typeName}", ""},
		{http.MethodPut, "/v3/{ledgerName}/account-types/default-enforcement-mode", `{"enforcementMode":"AUDIT"}`},
		{http.MethodPost, "/v3/{ledgerName}/prepared-queries", `{"name":"query","target":"ACCOUNTS"}`},
		{http.MethodPut, "/v3/{ledgerName}/prepared-queries/{queryName}", `{"filter":{"$exists":{"metadata":"approved"}}}`},
		{http.MethodDelete, "/v3/{ledgerName}/prepared-queries/{queryName}", ""},
	}
	failures := []struct {
		name, reason, message string
		failure               error
	}{
		{"disk gate", "WRITES_BLOCKED_DISK_FULL", "writes blocked: disk usage exceeds threshold", domain.ErrWritesBlockedDiskFull},
		{"sequence allocator", "SEQUENCE_EXHAUSTED", "logSequence exhausted: cannot allocate another identifier", &domain.ErrSequenceExhausted{Counter: domain.SequenceCounterLog}},
	}
	for _, operation := range operations {
		t.Run(operation.method+" "+operation.path, func(t *testing.T) {
			t.Parallel()
			for _, failure := range failures {
				for _, forwarded := range []bool{false, true} {
					name := failure.name
					if forwarded {
						name += " forwarded"
					}
					t.Run(name, func(t *testing.T) {
						t.Parallel()
						backendError := failure.failure
						if forwarded {
							wire, err := status.New(codes.ResourceExhausted, failure.message).WithDetails(&errdetails.ErrorInfo{
								Domain: "ledger", Reason: failure.reason,
							})
							require.NoError(t, err)
							backendError = grpcerr.FromStatusError(wire.Err())
						}
						backend := NewMockBackend(gomock.NewController(t))
						backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, backendError)
						handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
						path := strings.NewReplacer(
							"{ledgerName}", "ledger1", "{name}", "script", "{canonicalId}", "metadata:TARGET_TYPE_ACCOUNT:color",
							"{transactionId}", "1", "{address}", "alice", "{key}", "approved", "{targetType}", "account",
							"{typeName}", "customers", "{queryName}", "query",
						).Replace(operation.path)
						r := httptest.NewRequest(operation.method, "http://localhost:9000"+path, strings.NewReader(operation.body))
						r.Header.Set("Content-Type", "application/json")
						w := httptest.NewRecorder()
						handler.ServeHTTP(w, r)
						require.Equal(t, http.StatusTooManyRequests, w.Code)
						require.Equal(t, "application/json", w.Header().Get("Content-Type"))
						require.Empty(t, w.Header().Get("Retry-After"), "429 does not promise a retry interval")
						var body map[string]string
						require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
						require.Equal(t, map[string]string{"errorCode": failure.reason, "errorMessage": failure.message}, body)
						validateOpenAPIResponse(t, doc, r, w)
					})
				}
			}
		})
	}
	// Reads do not traverse the write-admission gate. Bulk owns a distinct
	// processing envelope and must not share the request-level JSON component.
	for path, item := range doc.Paths.Map() {
		for method, operation := range item.Operations() {
			if method == http.MethodGet || strings.HasSuffix(path, "/execute") {
				require.Nil(t, operation.Responses.Status(http.StatusTooManyRequests), "%s %s has no demonstrated exhaustion branch", method, path)
			}
		}
	}
	bulk := doc.Paths.Value("/v3/{ledgerName}/bulk").Post.Responses.Status(http.StatusTooManyRequests).Value
	require.Equal(t, "#/components/schemas/BulkResponse", bulk.Content["application/json"].Schema.Ref)
}
