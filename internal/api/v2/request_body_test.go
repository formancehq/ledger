package v2

import (
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/transport/api"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func TestOptionalRequestBodies(t *testing.T) {
	t.Parallel()

	metadataBody := `{"metadata":{"foo":"bar"}}`
	for _, tc := range []struct {
		name      string
		body      string
		metadata  metadata.Metadata
		status    int
		errorCode string
	}{
		{name: "empty"},
		{name: "metadata", body: metadataBody, metadata: metadata.Metadata{"foo": "bar"}},
		{
			name: "exact limit", body: metadataBody + strings.Repeat(" ", int(common.MaxJSONBodySize)-len(metadataBody)),
			metadata: metadata.Metadata{"foo": "bar"},
		},
		{name: "malformed", body: "{", status: http.StatusBadRequest, errorCode: common.ErrValidation},
		{name: "whitespace", body: " ", status: http.StatusBadRequest, errorCode: common.ErrValidation},
		{name: "trailing JSON", body: "{}{}", status: http.StatusBadRequest, errorCode: common.ErrValidation},
		{
			name: "over limit", body: strings.Repeat(" ", int(common.MaxJSONBodySize)+1),
			status: http.StatusRequestEntityTooLarge, errorCode: common.ErrRequestBodyTooLarge,
		},
	} {
		for _, transfer := range []string{"known length", "chunked"} {
			for _, endpoint := range []string{"create ledger", "revert transaction"} {
				t.Run(endpoint+"/"+transfer+"/"+tc.name, func(t *testing.T) {
					revert := endpoint == "revert transaction"
					systemController, ledgerController := newTestingSystemController(t, revert)
					path := "/xxx"
					status := http.StatusNoContent
					if revert {
						path = "/xxx/transactions/0/revert"
						status = http.StatusCreated
					}
					if tc.status != 0 {
						status = tc.status
					} else if revert {
						ledgerController.EXPECT().RevertTransaction(gomock.Any(), ledgercontroller.Parameters[ledgercontroller.RevertTransaction]{
							Input: ledgercontroller.RevertTransaction{Metadata: tc.metadata},
						}).Return(&ledger.Log{}, &ledger.RevertedTransaction{
							RevertTransaction: ledger.NewTransaction().WithPostings(
								ledger.NewPosting("world", "bank", "USD", big.NewInt(100)),
							),
						}, false, nil)
					} else {
						systemController.EXPECT().CreateLedger(gomock.Any(), "xxx", ledger.Configuration{Metadata: tc.metadata}).Return(nil)
					}

					request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
					if transfer == "chunked" {
						request.ContentLength = -1
						request.TransferEncoding = []string{"chunked"}
					}
					response := httptest.NewRecorder()
					NewRouter(systemController, jwt.NewNoAuth(), "develop").ServeHTTP(response, request)

					require.Equal(t, status, response.Code, response.Body.String())
					if tc.errorCode != "" {
						var apiError api.ErrorResponse
						api.Decode(t, response.Body, &apiError)
						require.Equal(t, tc.errorCode, apiError.ErrorCode)
					}
				})
			}
		}
	}
}

func TestQueryRequestBodyErrors(t *testing.T) {
	t.Parallel()

	for _, endpoint := range []struct {
		name   string
		method string
		path   string
	}{
		{name: "ledgers", method: http.MethodGet, path: "/"},
		{name: "accounts", method: http.MethodGet, path: "/xxx/accounts"},
		{name: "count accounts", method: http.MethodHead, path: "/xxx/accounts"},
		{name: "transactions", method: http.MethodGet, path: "/xxx/transactions"},
		{name: "count transactions", method: http.MethodHead, path: "/xxx/transactions"},
		{name: "logs", method: http.MethodGet, path: "/xxx/logs"},
		{name: "schemas", method: http.MethodGet, path: "/xxx/schemas"},
		{name: "balances", method: http.MethodGet, path: "/xxx/aggregate/balances"},
		{name: "volumes", method: http.MethodGet, path: "/xxx/volumes"},
	} {
		for _, tc := range []struct {
			name      string
			body      string
			query     string
			status    int
			errorCode string
		}{
			{
				name: "over limit", body: strings.Repeat(" ", int(common.MaxJSONBodySize)+1),
				status: http.StatusRequestEntityTooLarge, errorCode: common.ErrRequestBodyTooLarge,
			},
			{name: "malformed query", body: "{", status: http.StatusBadRequest, errorCode: common.ErrValidation},
			{name: "invalid date", query: "?pit=invalid", status: http.StatusBadRequest, errorCode: common.ErrValidation},
		} {
			for _, transfer := range []string{"known length", "chunked"} {
				t.Run(endpoint.name+"/"+transfer+"/"+tc.name, func(t *testing.T) {
					systemController, _ := newTestingSystemController(t, endpoint.path != "/")
					request := httptest.NewRequest(endpoint.method, endpoint.path+tc.query, strings.NewReader(tc.body))
					if transfer == "chunked" {
						request.ContentLength = -1
						request.TransferEncoding = []string{"chunked"}
					}
					response := httptest.NewRecorder()
					NewRouter(systemController, jwt.NewNoAuth(), "develop").ServeHTTP(response, request)

					require.Equal(t, tc.status, response.Code, response.Body.String())
					var apiError api.ErrorResponse
					api.Decode(t, response.Body, &apiError)
					require.Equal(t, tc.errorCode, apiError.ErrorCode)
				})
			}
		}
	}
}
