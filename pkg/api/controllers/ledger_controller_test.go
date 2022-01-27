package controllers_test

import (
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func TestGetStats(t *testing.T) {
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
					Destination: "boc",
					Amount:      100,
					Asset:       "USD",
				},
			},
		})
		assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

		rsp = internal.GetStats(h)
		assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

		stats := ledger.Stats{}
		internal.DecodeSingleResponse(t, rsp.Body, &stats)
		assert.EqualValues(t, ledger.Stats{
			Transactions: 2,
			Accounts:     3,
		}, stats)
	})
}
