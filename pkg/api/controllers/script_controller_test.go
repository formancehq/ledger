package controllers_test

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
)

func TestScriptController(t *testing.T) {
	type testCase struct {
		name             string
		script           string
		expectedResponse controllers.ScriptResponse
	}

	cases := []testCase{
		{
			name: "nominal",
			script: `send [COIN 100] (
  source = @world
  destination = @centralbank
)
send [COIN 100] (
  source = @centralbank
  destination = @users:001
)`,
		},
		{
			name: "failure",
			script: `
send [COIN 100] (
  source = @centralbank
  destination = @users:001
)`,
			expectedResponse: controllers.ScriptResponse{
				ErrorResponse: sharedapi.ErrorResponse{
					ErrorCode:    ledger.ScriptErrorInsufficientFund,
					ErrorMessage: "account had insufficient funds",
				},
				Link: controllers.EncodeLink("account had insufficient funds"),
			},
		},
	}

	for _, c := range cases {
		internal.RunSubTest(t, c.name, fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					rec := httptest.NewRecorder()
					req := httptest.NewRequest("POST", "/"+uuid.New()+"/script", internal.Buffer(t, core.Script{
						Plain: c.script,
					}))
					req.Header.Set("Content-Type", "application/json")

					h.ServeHTTP(rec, req)

					assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
					res := controllers.ScriptResponse{}
					internal.Decode(t, rec.Body, &res)

					res.Transaction = nil
					assert.EqualValues(t, c.expectedResponse, res)
					return nil
				},
			})
		}))
	}
}

func TestScriptControllerPreview(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, h *api.API, driver storage.Driver) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				l := uuid.New()
				rec := httptest.NewRecorder()
				req := httptest.NewRequest("POST", "/"+l+"/script", internal.Buffer(t, core.Script{
					Plain: `send [COIN 100] (
  source = @world
  destination = @centralbank
)`,
				}))
				req.Header.Set("Content-Type", "application/json")
				values := url.Values{}
				values.Set("preview", "yes")
				req.URL.RawQuery = values.Encode()

				h.ServeHTTP(rec, req)

				assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
				res := controllers.ScriptResponse{}
				internal.Decode(t, rec.Body, &res)

				store, _, err := driver.GetStore(context.Background(), l, true)
				assert.NoError(t, err)

				cursor, err := store.GetTransactions(context.Background(), query.Query{})
				assert.NoError(t, err)
				assert.Len(t, cursor.Data, 0)
				return nil
			},
		})
	}))
}
