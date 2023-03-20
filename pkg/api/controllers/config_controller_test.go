package controllers_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/internal"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

func TestGetInfo(t *testing.T) {
	internal.RunTest(t, func(h chi.Router, driver storage.Driver) {
		rsp := internal.GetInfo(h)
		require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

		info := controllers.ConfigInfo{}
		require.NoError(t, json.NewDecoder(rsp.Body).Decode(&info))

		info.Config.LedgerStorage.Ledgers = []string{}
		require.EqualValues(t, controllers.ConfigInfo{
			Server:  "ledger",
			Version: "latest",
			Config: &controllers.Config{
				LedgerStorage: &controllers.LedgerStorage{
					Driver:  driver.Name(),
					Ledgers: []string{},
				},
			},
		}, info)
	})
}
