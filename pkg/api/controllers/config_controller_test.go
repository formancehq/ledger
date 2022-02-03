package controllers_test

import (
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/config"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func TestGetInfo(t *testing.T) {
	internal.RunTest(t, func(h *api.API) {
		rsp := internal.GetInfo(h)
		assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)

		info := config.ConfigInfo{}
		internal.DecodeSingleResponse(t, rsp.Body, &info)
		assert.EqualValues(t, config.ConfigInfo{
			Server:  "numary-ledger",
			Version: "latest",
			Config: &config.Config{
				LedgerStorage: &config.LedgerStorage{
					Driver:  "sqlite",
					Ledgers: []string{"quickstart"},
				},
			},
		}, info)
	})
}
