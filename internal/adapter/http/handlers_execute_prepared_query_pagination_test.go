package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestExecutePreparedQueryRoutePagination(t *testing.T) {
	t.Parallel()
	for _, reverse := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			bodySize uint64
			query    string
			mode     string
			expected uint32
			status   int
		}{
			{"default", 0, "", "", 100, 200},
			{"maximum", 1000, "", "LIST", 1000, 200},
			{"over-maximum", 1001, "", "LIST", 1000, 200},
			{"uint32-maximum", 4294967295, "", "LIST", 1000, 200},
			{"empty-query-override", 1001, "?pageSize=", "LIST", 100, 200},
			{"zero-query-override", 1001, "?pageSize=0", "LIST", 100, 200},
			{"query-override", 1001, "?pageSize=42", "LIST", 42, 200},
			{"maximum-query-override", 42, "?pageSize=1000", "LIST", 1000, 200},
			{"over-maximum-query-override", 42, "?pageSize=1001", "LIST", 1000, 200},
			{"uint32-maximum-query-override", 42, "?pageSize=4294967295", "LIST", 1000, 200},
			{"query-overflow", 42, "?pageSize=4294967296", "LIST", 0, 400},
			{"aggregate", 4294967295, "", "AGGREGATE_VOLUMES", 4294967295, 200},
			{"overflow", 4294967296, "", "LIST", 0, 400},
		} {
			t.Run(fmt.Sprintf("%s/reverse=%t", tc.name, reverse), func(t *testing.T) {
				t.Parallel()
				backend := NewMockBackend(gomock.NewController(t))
				if tc.status == http.StatusOK {
					backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
						func(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
							require.Equal(t, tc.expected, req.GetPageSize())
							require.Equal(t, reverse, req.GetReverse())
							require.Equal(t, "ledger1", req.GetLedger())
							require.Equal(t, "accounts", req.GetQueryName())
							expectedMode := ledgerpb.QueryMode_QUERY_MODE_LIST
							if tc.mode == "AGGREGATE_VOLUMES" {
								expectedMode = ledgerpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES
							}
							require.Equal(t, expectedMode, req.GetMode())

							return &servicepb.ExecutePreparedQueryResponse{}, nil
						})
				}
				mapping := internalauth.DefaultMapping("ledger")
				mapping[internalauth.ScopeMappingAnonymousKey] = []internalauth.Scope{internalauth.ScopeQueriesRead}
				handler := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{Enabled: true, ScopeMapping: mapping}, version.Info{})
				body := fmt.Sprintf(`{"pageSize":%d,"mode":%q,"reverse":%t}`, tc.bodySize, tc.mode, reverse)
				request := httptest.NewRequest(http.MethodPost, "/v3/ledger1/prepared-queries/accounts/execute"+tc.query, strings.NewReader(body))
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				require.Equal(t, tc.status, response.Code, response.Body.String())
			})
		}
	}
}
