package http

import (
	"context"
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

// Numscript, prepared-query and account-type names are free-form identifiers
// in a path segment, so they follow the same single-decoding rule.
func TestNamedResourceRouting(t *testing.T) {
	t.Parallel()
	routes := []struct {
		name, method, path string
		status             int
		expect             func(t *testing.T, backend *MockBackend, captured *string)
	}{
		{"numscript", http.MethodGet, "/numscripts/", http.StatusOK, func(_ *testing.T, backend *MockBackend, captured *string) {
			backend.EXPECT().GetNumscript(gomock.Any(), "ledger1", gomock.Any(), "").DoAndReturn(
				func(_ context.Context, _, name, _ string) (*commonpb.NumscriptInfo, error) {
					*captured = name

					return &commonpb.NumscriptInfo{}, nil
				})
		}},
		{"prepared_query", http.MethodDelete, "/prepared-queries/", http.StatusNoContent, func(t *testing.T, backend *MockBackend, captured *string) {
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
				require.Len(t, req.GetUnsigned().GetRequests(), 1)
				*captured = req.GetUnsigned().GetRequests()[0].GetDeletePreparedQuery().GetName()

				return &domain.ApplyResult{Logs: []*commonpb.Log{{}}}, nil
			})
		}},
		{"account_type", http.MethodDelete, "/account-types/", http.StatusNoContent, func(t *testing.T, backend *MockBackend, captured *string) {
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
				require.Len(t, req.GetUnsigned().GetRequests(), 1)
				*captured = req.GetUnsigned().GetRequests()[0].GetRemoveAccountType().GetName()

				return &domain.ApplyResult{Logs: []*commonpb.Log{{}}}, nil
			})
		}},
	}
	for _, route := range routes {
		for _, name := range pathIdentifierEncodings {
			t.Run(route.name+"/"+name.name, func(t *testing.T) {
				t.Parallel()
				var captured string
				backend := NewMockBackend(gomock.NewController(t))
				route.expect(t, backend, &captured)
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				req := httptest.NewRequest(route.method, "/v3/ledger1"+route.path+name.encoded, nil)
				require.Equal(t, name.rawPath, req.URL.RawPath != "")
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				require.Equal(t, route.status, rec.Code, rec.Body.String())
				require.Equal(t, name.decoded, captured)
			})
		}
	}
}
