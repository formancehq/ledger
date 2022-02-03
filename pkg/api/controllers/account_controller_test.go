package controllers_test

import (
	"encoding/json"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func TestGetAccounts(t *testing.T) {

	internal.RunTest(t, func(h *api.API) {

		rsp := internal.PostTransaction(t, h, core.TransactionData{
			Postings: core.Postings{
				{
					Source:      "world",
					Destination: "alice",
					Amount:      100,
					Asset:       "USD",
				},
			},
		})
		assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

		rsp = internal.PostTransaction(t, h, core.TransactionData{
			Postings: core.Postings{
				{
					Source:      "world",
					Destination: "bob",
					Amount:      100,
					Asset:       "USD",
				},
			},
		})
		assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

		rsp = internal.GetAccounts(h)
		assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

		cursor := internal.DecodeCursorResponse(t, rsp.Body, core.Account{})
		assert.EqualValues(t, 3, cursor.Total)
		assert.Len(t, cursor.Data, 3)
	})
}

func TestGetAccount(t *testing.T) {
	internal.RunTest(t, func(h *api.API) {
		rsp := internal.PostTransaction(t, h, core.TransactionData{
			Postings: core.Postings{
				{
					Source:      "world",
					Destination: "alice",
					Amount:      100,
					Asset:       "USD",
				},
			},
		})
		assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

		rsp = internal.PostAccountMetadata(t, h, "alice", core.Metadata{
			"foo": json.RawMessage(`"bar"`),
		})
		assert.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

		rsp = internal.GetAccount(h, "alice")
		assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

		act := core.Account{}
		internal.DecodeSingleResponse(t, rsp.Body, &act)

		assert.EqualValues(t, core.Account{
			Address: "alice",
			Type:    "",
			Balances: map[string]int64{
				"USD": 100,
			},
			Volumes: map[string]map[string]int64{
				"USD": {
					"input": 100,
				},
			},
			Metadata: core.Metadata{
				"foo": json.RawMessage(`"bar"`),
			},
		}, act)
	})
}
