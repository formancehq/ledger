package http

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// pathIdentifierEncodings lists segments that select the identifier users:a,
// plus double encodings that must keep a literal %3A. RawPath records whether
// Go keeps the raw form, which makes chi capture the still-escaped segment.
var pathIdentifierEncodings = []struct {
	name, encoded, decoded string
	rawPath                bool
}{
	{"plain", "users:a", "users:a", false},
	{"colon", "users%3Aa", "users:a", true},
	{"escaped_letter", "%75sers:a", "users:a", true},
	{"double_encoded_path", "users%253Aa", "users%3Aa", false},
	{"double_encoded_raw_path", "%75sers%253Aa", "users%3Aa", true},
}

// Exercise NewHandler so URL.Path/RawPath and chi's route segmentation are real.
// The mock captures the address that reaches the backend boundary.
func TestAccountAddressRouting(t *testing.T) {
	t.Parallel()
	applied := func(addr func(*servicepb.Request) string) func(*testing.T, *MockBackend, *string) {
		return func(t *testing.T, backend *MockBackend, captured *string) {
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
				require.Len(t, req.GetUnsigned().GetRequests(), 1)
				*captured = addr(req.GetUnsigned().GetRequests()[0])

				return &domain.ApplyResult{Logs: []*commonpb.Log{{}}}, nil
			})
		}
	}
	routes := []struct {
		name, method, suffix, body string
		status                     int
		expect                     func(t *testing.T, backend *MockBackend, captured *string)
	}{
		{"get", http.MethodGet, "", "", http.StatusOK, func(_ *testing.T, backend *MockBackend, captured *string) {
			backend.EXPECT().GetLedgerByName(gomock.Any(), "ledger1").Return(&commonpb.LedgerInfo{Name: "ledger1"}, nil)
			backend.EXPECT().GetAccount(gomock.Any(), "ledger1", gomock.Any(), ctrl.GetAccountOptions{}).DoAndReturn(
				func(_ context.Context, _ string, addr string, _ ctrl.GetAccountOptions) (*commonpb.Account, error) {
					*captured = addr

					return &commonpb.Account{Address: addr}, nil
				})
		}},
		{"save_metadata", http.MethodPost, "/metadata", `{"role":"admin"}`, http.StatusNoContent, applied(func(r *servicepb.Request) string {
			return r.GetApply().GetAction().GetAddMetadata().GetTarget().GetAccount().GetAddr()
		})},
		{"delete_metadata", http.MethodDelete, "/metadata/role", "", http.StatusNoContent, applied(func(r *servicepb.Request) string {
			return r.GetApply().GetAction().GetDeleteMetadata().GetTarget().GetAccount().GetAddr()
		})},
	}
	for _, route := range routes {
		for _, address := range pathIdentifierEncodings {
			t.Run(route.name+"/"+address.name, func(t *testing.T) {
				t.Parallel()
				var captured string
				backend := NewMockBackend(gomock.NewController(t))
				route.expect(t, backend, &captured)
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				var body io.Reader
				if route.body != "" {
					body = strings.NewReader(route.body)
				}
				req := httptest.NewRequest(route.method, "/v3/ledger1/accounts/"+address.encoded+route.suffix, body)
				require.Equal(t, address.rawPath, req.URL.RawPath != "")
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				require.Equal(t, route.status, rec.Code, rec.Body.String())
				require.Equal(t, address.decoded, captured)
			})
		}
	}
}

// namedResourceEncodings adds encoded slashes, which free-form names may hold
// but account addresses may not.
var namedResourceEncodings = append([]struct {
	name, encoded, decoded string
	rawPath                bool
}{
	{"slash_upper", "reports%2Fgold", "reports/gold", true},
	{"slash_lower", "reports%2fgold", "reports/gold", true},
	{"double_encoded_slash", "reports%252Fgold", "reports%2Fgold", false},
}, pathIdentifierEncodings...)

// Numscript, prepared-query and account-type names are free-form identifiers
// in a path segment, so they follow the same single-decoding rule.
func TestNamedResourceRouting(t *testing.T) {
	t.Parallel()
	applied := func(name func(*servicepb.Request) string, logs ...*commonpb.Log) func(*testing.T, *MockBackend, *string, string) {
		return func(t *testing.T, backend *MockBackend, captured *string, _ string) {
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
				require.Len(t, req.GetUnsigned().GetRequests(), 1)
				*captured = name(req.GetUnsigned().GetRequests()[0])
				if len(logs) == 0 {
					return &domain.ApplyResult{Logs: []*commonpb.Log{{}}}, nil
				}

				return &domain.ApplyResult{Logs: logs}, nil
			})
		}
	}
	savedNumscript := &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SavedNumscript{SavedNumscript: &commonpb.SavedNumscriptLog{
			Info: &commonpb.NumscriptInfo{Name: "saved", Version: "1.0.0"},
		}},
	}}
	routes := []struct {
		name, method, prefix, suffix, body string
		status                             int
		// expect registers the backend call and records the name it receives.
		// want is the decoded name, for backends that answer by name lookup.
		expect func(t *testing.T, backend *MockBackend, captured *string, want string)
	}{
		{"numscript_get", http.MethodGet, "/numscripts/", "", "", http.StatusOK, func(_ *testing.T, backend *MockBackend, captured *string, _ string) {
			backend.EXPECT().GetNumscript(gomock.Any(), "ledger1", gomock.Any(), "").DoAndReturn(
				func(_ context.Context, _, name, _ string) (*commonpb.NumscriptInfo, error) {
					*captured = name

					return &commonpb.NumscriptInfo{}, nil
				})
		}},
		{"numscript_save", http.MethodPut, "/numscripts/", "", `{"content":"send [USD 1] (source = @world destination = @bank)","version":"1.0.0"}`, http.StatusCreated, applied(func(r *servicepb.Request) string {
			return r.GetSaveNumscript().GetName()
		}, savedNumscript)},
		{"prepared_query_delete", http.MethodDelete, "/prepared-queries/", "", "", http.StatusNoContent, applied(func(r *servicepb.Request) string {
			return r.GetDeletePreparedQuery().GetName()
		})},
		{"prepared_query_execute", http.MethodPost, "/prepared-queries/", "/execute", "", http.StatusOK, func(_ *testing.T, backend *MockBackend, captured *string, _ string) {
			backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
					*captured = req.GetQueryName()

					return &servicepb.ExecutePreparedQueryResponse{}, nil
				})
		}},
		// The handler looks the name up in LedgerInfo, where only the decoded
		// name exists: a still-escaped lookup answers 404. The name is read
		// back from the response.
		{"account_type_get", http.MethodGet, "/account-types/", "", "", http.StatusOK, func(_ *testing.T, backend *MockBackend, _ *string, want string) {
			backend.EXPECT().GetLedgerByName(gomock.Any(), "ledger1").Return(&commonpb.LedgerInfo{
				Name:         "ledger1",
				AccountTypes: map[string]*commonpb.AccountType{want: {Name: want, Pattern: "users:{id}"}},
			}, nil)
		}},
		{"account_type_remove", http.MethodDelete, "/account-types/", "", "", http.StatusNoContent, applied(func(r *servicepb.Request) string {
			return r.GetRemoveAccountType().GetName()
		})},
	}
	for _, route := range routes {
		for _, name := range namedResourceEncodings {
			t.Run(route.name+"/"+name.name, func(t *testing.T) {
				t.Parallel()
				var captured string
				backend := NewMockBackend(gomock.NewController(t))
				route.expect(t, backend, &captured, name.decoded)
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				var body io.Reader
				if route.body != "" {
					body = strings.NewReader(route.body)
				}
				req := httptest.NewRequest(route.method, "/v3/ledger1"+route.prefix+name.encoded+route.suffix, body)
				require.Equal(t, name.rawPath, req.URL.RawPath != "")
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				require.Equal(t, route.status, rec.Code, rec.Body.String())
				if route.name == "account_type_get" {
					var resp struct {
						Data struct {
							Name string `json:"name"`
						} `json:"data"`
					}
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
					captured = resp.Data.Name
				}
				require.Equal(t, name.decoded, captured)
			})
		}
	}
}
