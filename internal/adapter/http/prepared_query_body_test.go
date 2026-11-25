package http

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Each route must reject a malformed suffix before reading or mutating a query.
func TestPreparedQueryRoutesConsumeFullBody(t *testing.T) {
	t.Parallel()

	for _, route := range []struct {
		name, method, path, body string
		success                  int
	}{
		{"create", http.MethodPost, "/v3/ledger1/prepared-queries", `{"name":"query","target":"ACCOUNTS"}`, http.StatusNoContent},
		{"update", http.MethodPut, "/v3/ledger1/prepared-queries/query", `{"target":"ACCOUNTS","filter":null}`, http.StatusNoContent},
		{"execute", http.MethodPost, "/v3/ledger1/prepared-queries/query/execute", `{"pageSize":10}`, http.StatusOK},
	} {
		for _, tc := range []struct {
			name, suffix string
			status       int
			errorCode    string
		}{
			{"whitespace", " \t\r\n", route.success, ""},
			{"second value", " {}", http.StatusBadRequest, "INVALID_REQUEST"},
			{"trailing junk", " !", http.StatusBadRequest, "INVALID_REQUEST"},
			{"exact size", strings.Repeat(" ", int(defaultMaxBodySize)-len(route.body)), route.success, ""},
			{"oversized whitespace", strings.Repeat(" ", int(defaultMaxBodySize)-len(route.body)+1), http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE"},
		} {
			t.Run(route.name+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				backend := NewMockBackend(gomock.NewController(t))
				// No expectation on invalid input: any backend call fails the test.
				if tc.status == http.StatusNoContent {
					backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(&domain.ApplyResult{}, nil)
				}
				if tc.status == http.StatusOK {
					backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).Return(&servicepb.ExecutePreparedQueryResponse{}, nil)
				}
				req := httptest.NewRequest(route.method, route.path, strings.NewReader(route.body+tc.suffix))
				req.Header.Set("Content-Type", "application/json")
				// A streaming request need not advertise its size.
				req.ContentLength = -1
				w := httptest.NewRecorder()
				NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil).ServeHTTP(w, req)
				require.Equal(t, tc.status, w.Code, w.Body.String())
				if tc.errorCode != "" {
					require.Equal(t, tc.errorCode, decodeResponse[ErrorResponse](t, w).ErrorCode)
				}
			})
		}
	}
}

func TestExecutePreparedQueryRouteParameterValues(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, raw string
		want      *commonpb.ParameterValue
	}{
		{"above float precision", "9007199254740993", &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 9007199254740993}}},
		{"maximum signed", "9223372036854775807", &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: math.MaxInt64}}},
		{"maximum unsigned", "18446744073709551615", &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: math.MaxUint64}}},
		{"minimum signed", "-9223372036854775808", &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Int64Value{Int64Value: math.MinInt64}}},
		{"exact integral exponent", "9.007199254740993e15", &commonpb.ParameterValue{Value: &commonpb.ParameterValue_Uint64Value{Uint64Value: 9007199254740993}}},
		{"string stays string", `"9007199254740993"`, &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: "9007199254740993"}}},
		{"empty string stays string", `""`, &commonpb.ParameterValue{Value: &commonpb.ParameterValue_StringValue{StringValue: ""}}},
		{"false stays boolean", "false", &commonpb.ParameterValue{Value: &commonpb.ParameterValue_BoolValue{BoolValue: false}}},
		{"unsigned overflow", "18446744073709551616", nil},
		{"signed overflow", "-9223372036854775809", nil},
		{"fraction rounded by float", "1.000000000000000001", nil},
		{"null", "null", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := NewMockBackend(gomock.NewController(t))
			if tc.want != nil {
				backend.EXPECT().ExecutePreparedQuery(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
						got := req.GetParameters()["value"]
						require.True(t, proto.Equal(tc.want, got), "got %v, want %v", got, tc.want)

						return &servicepb.ExecutePreparedQueryResponse{}, nil
					})
			}
			req := httptest.NewRequest(http.MethodPost, "/v3/ledger1/prepared-queries/query/execute", strings.NewReader(`{"parameters":{"value":`+tc.raw+`}}`))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}, nil).ServeHTTP(w, req)
			if tc.want != nil {
				require.Equal(t, http.StatusOK, w.Code, w.Body.String())

				return
			}
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			failure := decodeResponse[ErrorResponse](t, w)
			require.Equal(t, "INVALID_PARAMETERS", failure.ErrorCode)
			require.Contains(t, failure.ErrorMessage, `parameter "value"`)
		})
	}
}
