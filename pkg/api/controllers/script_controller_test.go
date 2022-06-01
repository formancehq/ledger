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
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
)

func TestPostScript(t *testing.T) {
	type testCase struct {
		name             string
		script           string
		expectedResponse controllers.ScriptResponse
	}

	script1 := `
	send [COIN 100] (
	  source = @world
	  destination = @centralbank
	)
	send [COIN 100] (
	  source = @centralbank
	  destination = @users:001
	)`

	script2 := `
	send [COIN 100] (
	  source = @centralbank
	  destination = @users:001
	)`

	testCases := []testCase{
		{
			name:   "nominal",
			script: script1,
		},
		{
			name:   "failure",
			script: script2,
			expectedResponse: controllers.ScriptResponse{
				ErrorResponse: sharedapi.ErrorResponse{
					ErrorCode:    ledger.ScriptErrorInsufficientFund,
					ErrorMessage: "account had insufficient funds",
				},
				Link: controllers.EncodeLink("account had insufficient funds"),
			},
		},
	}

	for _, tc := range testCases {
		internal.RunSubTest(t, tc.name, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					rsp := internal.PostScript(t, api, core.Script{
						Plain: tc.script,
					}, url.Values{})
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
				values := url.Values{}
				values.Set("preview", "true")

				rsp := internal.PostScript(t, api, core.Script{
					Plain: script,
				}, values)

				assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				res := controllers.ScriptResponse{}
				internal.Decode(t, rsp.Body, &res)

				store := internal.GetStore(t, driver, ctx)
				cursor, err := store.GetTransactions(ctx, query.New())
				assert.NoError(t, err)
				assert.Len(t, cursor.Data, 0)
				return nil
			},
		})
	}))
}
