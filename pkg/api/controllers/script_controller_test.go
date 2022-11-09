package controllers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
)

func TestPostScript(t *testing.T) {
	type testCase struct {
		name             string
		script           core.Script
		expectedResponse controllers.ScriptResponse
	}

	testCases := []testCase{
		{
			name: "nominal",
			script: core.Script{
				Plain: `
				send [COIN 100] (
				  source = @world
				  destination = @centralbank
				)
				send [COIN 100] (
				  source = @centralbank
				  destination = @users:001
				)`,
			},
		},
		{
			name: "failure with insufficient funcs",
			script: core.Script{
				Plain: `
				send [COIN 100] (
				  source = @centralbank
				  destination = @users:001
				)`,
			},
			expectedResponse: controllers.ScriptResponse{
				ErrorResponse: sharedapi.ErrorResponse{
					ErrorCode:    ledger.ScriptErrorInsufficientFund,
					ErrorMessage: "account had insufficient funds",
				},
				Link: controllers.EncodeLink("account had insufficient funds"),
			},
		},
		{
			name: "failure with metadata override",
			script: core.Script{
				Plain: `
				set_tx_meta("priority", "low")

				send [USD/2 99] (
					source=@world
					destination=@user:001
				)`,
				Metadata: core.Metadata{
					"priority": json.RawMessage(`"high"`),
				},
			},
			expectedResponse: controllers.ScriptResponse{
				ErrorResponse: sharedapi.ErrorResponse{
					ErrorCode:    ledger.ScriptErrorMetadataOverride,
					ErrorMessage: "cannot override metadata from script",
				},
				Link: controllers.EncodeLink("cannot override metadata from script"),
			},
		},
	}

	for _, tc := range testCases {
		internal.RunSubTest(t, tc.name, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					rsp := internal.PostScript(t, api, tc.script, url.Values{})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

					res := controllers.ScriptResponse{}
					assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &res))

					res.Transaction = nil
					assert.EqualValues(t, tc.expectedResponse, res)
					return nil
				},
			})
		}))
	}
}

func TestPostScriptPreview(t *testing.T) {
	script := `
	send [COIN 100] (
	  source = @world
	  destination = @centralbank
	)`

	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				store := internal.GetStore(t, driver, ctx)

				t.Run("true", func(t *testing.T) {
					values := url.Values{}
					values.Set("preview", "true")

					rsp := internal.PostScript(t, api, core.Script{
						Plain: script,
					}, values)

					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					res := controllers.ScriptResponse{}
					internal.Decode(t, rsp.Body, &res)

					cursor, err := store.GetTransactions(ctx, *storage.NewTransactionsQuery())
					assert.NoError(t, err)
					assert.Len(t, cursor.Data, 0)
				})

				t.Run("false", func(t *testing.T) {
					values := url.Values{}
					values.Set("preview", "false")

					rsp := internal.PostScript(t, api, core.Script{
						Plain: script,
					}, values)

					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					res := controllers.ScriptResponse{}
					internal.Decode(t, rsp.Body, &res)

					cursor, err := store.GetTransactions(ctx, *storage.NewTransactionsQuery())
					assert.NoError(t, err)
					assert.Len(t, cursor.Data, 1)
				})

				return nil
			},
		})
	}))
}

func TestPostScriptWithReference(t *testing.T) {

	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				reference := "order_1234"
				rsp := internal.PostScript(t, api, core.Script{
					Plain: `
						send [COIN 100] (
						  	source = @world
						  	destination = @centralbank
						)`,
					Reference: reference,
				}, url.Values{})
				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				res := controllers.ScriptResponse{}
				assert.NoError(t, json.Unmarshal(rsp.Body.Bytes(), &res))
				assert.Equal(t, reference, res.Transaction.Reference)

				store := internal.GetStore(t, driver, ctx)
				cursor, err := store.GetTransactions(ctx, *storage.NewTransactionsQuery())
				assert.NoError(t, err)
				assert.Len(t, cursor.Data, 1)
				assert.Equal(t, reference, cursor.Data[0].Reference)

				return nil
			},
		})
	}))
}

func TestPostScriptConflict(t *testing.T) {
	script := `
	send [COIN 100] (
	  source = @world
	  destination = @centralbank
	)`

	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				t.Run("first should succeed", func(t *testing.T) {
					rsp := internal.PostScript(t, api, core.Script{
						Plain:     script,
						Reference: "1234",
					}, url.Values{})

					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					res := controllers.ScriptResponse{}
					internal.Decode(t, rsp.Body, &res)
					assert.Equal(t, "", res.ErrorCode)
					assert.Equal(t, "", res.ErrorMessage)
					assert.NotNil(t, res.Transaction)
				})

				t.Run("second should fail", func(t *testing.T) {
					rsp := internal.PostScript(t, api, core.Script{
						Plain:     script,
						Reference: "1234",
					}, url.Values{})

					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					res := controllers.ScriptResponse{}
					internal.Decode(t, rsp.Body, &res)
					assert.Equal(t, controllers.ErrConflict, res.ErrorCode)
				})

				return nil
			},
		})
	}))
}
