package api

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/logging"
	"github.com/numary/ledger/pkg/storage"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"net/http"
	"net/http/httptest"
	"testing"
)

func withNewModule(t *testing.T, options ...fx.Option) {
	module := Module(Config{
		StorageDriver: viper.GetString("sqlite"),
		LedgerLister: controllers.LedgerListerFn(func(r *http.Request) []string {
			return []string{}
		}),
		Version: "latest",
	})
	ch := make(chan struct{})
	options = append([]fx.Option{
		module,
		ledger.ResolveModule(),
		storage.DefaultModule(),
		ledgertesting.TestingModule(),
		logging.LogrusModule(),
		fx.NopLogger,
	}, options...)
	options = append(options, fx.Invoke(func() {
		close(ch)
	}))

	app := fx.New(options...)
	select {
	case <-ch:
	default:
		assert.Fail(t, app.Err().Error())
	}
}

func TestAdditionalGlobalMiddleware(t *testing.T) {
	withNewModule(t,
		routes.ProvideMiddlewares(func() []gin.HandlerFunc {
			return []gin.HandlerFunc{
				func(context *gin.Context) {
					context.AbortWithError(418, errors.New(""))
				},
			}
		}),
		fx.Invoke(func(api *API) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/_info", nil)

			api.ServeHTTP(rec, req)
			assert.Equal(t, 418, rec.Code)
		}),
	)
}

func TestAdditionalPerLedgerMiddleware(t *testing.T) {
	withNewModule(t,
		routes.ProvidePerLedgerMiddleware(func() []gin.HandlerFunc {
			return []gin.HandlerFunc{
				func(context *gin.Context) {
					context.AbortWithError(418, errors.New(""))
				},
			}
		}),
		fx.Invoke(func(api *API) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/XXX/transactions", nil)

			api.ServeHTTP(rec, req)
			assert.Equal(t, 418, rec.Code)
		}),
	)
}

func TestCommitTransaction(t *testing.T) {

	type testCase struct {
		name               string
		transactions       []core.TransactionData
		expectedStatusCode int
		expectedErrorCode  string
	}
	testCases := []testCase{
		{
			name:               "nominal",
			expectedStatusCode: http.StatusOK,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      1000,
							Asset:       "USB",
						},
					},
				},
			},
		},
		{
			name:               "no-postings",
			expectedStatusCode: http.StatusBadRequest,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{},
				},
			},
			expectedErrorCode: controllers.ErrValidation,
		},
		{
			name:               "negative-amount",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  controllers.ErrValidation,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      -1000,
							Asset:       "USB",
						},
					},
				},
			},
		},
		{
			name:               "wrong-asset",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  controllers.ErrValidation,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      1000,
							Asset:       "@TOK",
						},
					},
				},
			},
		},
		{
			name:               "bad-address",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  controllers.ErrValidation,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "#fake",
							Amount:      1000,
							Asset:       "TOK",
						},
					},
				},
			},
		},
		{
			name:               "missing-funds",
			expectedStatusCode: http.StatusBadRequest,
			expectedErrorCode:  controllers.ErrInsufficientFund,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "foo",
							Destination: "bar",
							Amount:      1000,
							Asset:       "TOK",
						},
					},
				},
			},
		},
		{
			name:               "reference-conflict",
			expectedStatusCode: http.StatusConflict,
			expectedErrorCode:  controllers.ErrConflict,
			transactions: []core.TransactionData{
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bar",
							Amount:      1000,
							Asset:       "TOK",
						},
					},
					Reference: "ref",
				},
				{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bar",
							Amount:      1000,
							Asset:       "TOK",
						},
					},
					Reference: "ref",
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			withNewModule(t, fx.Invoke(func(api *API) {
				doRequest := func(tx core.TransactionData) *httptest.ResponseRecorder {
					data, err := json.Marshal(tx)
					assert.NoError(t, err)

					rec := httptest.NewRecorder()
					req := httptest.NewRequest(http.MethodPost, "/quickstart/transactions", bytes.NewBuffer(data))
					req.Header.Set("Content-Type", "application/json")

					api.ServeHTTP(rec, req)
					return rec
				}
				for i := 0; i < len(tc.transactions)-1; i++ {
					rsp := doRequest(tc.transactions[i])
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				}
				rsp := doRequest(tc.transactions[len(tc.transactions)-1])
				assert.Equal(t, tc.expectedStatusCode, rsp.Result().StatusCode)
			}))

		})
	}
}
