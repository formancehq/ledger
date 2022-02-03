package controllers_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/assert"
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
			expectedResponse: controllers.ScriptResponse{
				Txs: []ledger.CommitTransactionResult{
					{
						Transaction: core.Transaction{
							TransactionData: core.TransactionData{
								Postings: core.Postings{
									{
										Source:      "world",
										Destination: "centralbank",
										Amount:      100,
										Asset:       "COIN",
									},
									{
										Source:      "centralbank",
										Destination: "users:001",
										Amount:      100,
										Asset:       "COIN",
									},
								},
								Metadata: core.Metadata{},
							},
						},
					},
				},
			},
		},
		{
			name: "failure",
			script: `
send [COIN 100] (
  source = @centralbank
  destination = @users:001
)`,
			expectedResponse: controllers.ScriptResponse{
				ErrorResponse: controllers.ErrorResponse{
					ErrorCode:    controllers.ErrInternal,
					ErrorMessage: "account had insufficient funds",
				},
				Link: controllers.EncodeLink(errors.New("account had insufficient funds")),
			},
		},
	}

	for _, c := range cases {
		internal.RunSubTest(t, c.name, func(h *api.API) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest("POST", "/quickstart/script", internal.Buffer(t, core.Script{
				Plain: c.script,
			}))
			req.Header.Set("Content-Type", "application/json")

			h.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusOK, rec.Result().StatusCode)
			res := controllers.ScriptResponse{}
			internal.Decode(t, rec.Body, &res)

			for i := range res.Txs {
				res.Txs[i].Timestamp = ""
				res.Txs[i].Hash = ""
			}

			assert.EqualValues(t, c.expectedResponse, res)
		})
	}
}
