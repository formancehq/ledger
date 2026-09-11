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

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Exercise registered routes, including unknown-length revert bodies, and compare native
// integers at Apply, before any response JSON decoding can mask precision loss.
func TestMetadataIntegers_Routes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		token string
		want  any
	}{
		{"0", uint64(0)}, {"42", uint64(42)}, {"-7", int64(-7)},
		{"9007199254740991", uint64(9007199254740991)},
		{"9007199254740992", uint64(9007199254740992)},
		{"9007199254740993", uint64(9007199254740993)},
		{"-9007199254740991", int64(-9007199254740991)},
		{"-9007199254740992", int64(-9007199254740992)},
		{"-9007199254740993", int64(-9007199254740993)},
		{"9223372036854775807", uint64(9223372036854775807)},
		{"-9223372036854775808", int64(-9223372036854775808)},
		{"18446744073709551615", uint64(18446744073709551615)},
		{"1.0", uint64(1)}, {"1e3", uint64(1000)}, {"1e-400", nil}, {"1.5", nil}, {"9007199254740993.5", nil},
		{"-9223372036854775809", nil}, {"18446744073709551616", nil},
	}
	for _, route := range []string{"account", "create", "createAccount", "revert", "revertUnknownLength"} {
		for _, tc := range cases {
			t.Run(route+"/"+tc.token, func(t *testing.T) {
				t.Parallel()
				backend := NewMockBackend(gomock.NewController(t))
				if tc.want != nil {
					backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
						requests := req.GetUnsigned().GetRequests()
						require.Len(t, requests, 1)
						require.Equal(t, "ledger1", requests[0].GetApply().GetLedger())
						action := requests[0].GetApply().GetAction()
						var md map[string]*commonpb.MetadataValue
						logData := &commonpb.LedgerLogPayload{}
						switch route {
						case "account":
							cmd := action.GetAddMetadata()
							require.Equal(t, "users:001", cmd.GetTarget().GetAccount().GetAddr())
							md = cmd.GetMetadata()
						case "create", "createAccount":
							cmd := action.GetCreateTransaction()
							md = cmd.GetMetadata()
							if route == "createAccount" {
								md = cmd.GetAccountMetadata()["users:001"].GetValues()
							}
							require.Equal(t, tc.want, commonpb.MetadataValueToAny(cmd.GetAccountMetadata()["users:001"].GetValues()["count"]))
							logData.Payload = &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 1}}}
						case "revert", "revertUnknownLength":
							md = action.GetRevertTransaction().GetMetadata()
							logData.Payload = &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{RevertTransaction: &commonpb.Transaction{Id: 2}}}
						}
						require.Equal(t, tc.want, commonpb.MetadataValueToAny(md["count"]))

						return &domain.ApplyResult{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: logData}}}}}}}, nil
					})
				}
				path := "/v3/ledger1/accounts/users:001/metadata"
				body := fmt.Sprintf(`{"count":%s}`, tc.token)
				status := http.StatusNoContent
				switch route {
				case "create", "createAccount":
					path = "/v3/ledger1/transactions"
					body = fmt.Sprintf(`{"postings":[{"source":"world","destination":"users:001","asset":"USD","amount":1}],"metadata":{"count":%s},"accountMetadata":{"users:001":{"count":%s}}}`, tc.token, tc.token)
					if route == "createAccount" {
						body = fmt.Sprintf(`{"postings":[{"source":"world","destination":"users:001","asset":"USD","amount":1}],"accountMetadata":{"users:001":{"count":%s}}}`, tc.token)
					}
					status = http.StatusCreated
				case "revert", "revertUnknownLength":
					path = "/v3/ledger1/transactions/1/revert"
					body = fmt.Sprintf(`{"metadata":{"count":%s}}`, tc.token)
					status = http.StatusCreated
				}
				if tc.want == nil {
					status = http.StatusBadRequest
				}
				r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
				require.Positive(t, r.ContentLength)
				if route == "revertUnknownLength" {
					r.ContentLength = -1
				}
				w := httptest.NewRecorder()
				NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}).ServeHTTP(w, r)
				require.Equal(t, status, w.Code, w.Body.String())
				if tc.want == nil {
					require.Equal(t, "INVALID_REQUEST", decodeResponse[ErrorResponse](t, w).ErrorCode)
				}
			})
		}
	}
}
