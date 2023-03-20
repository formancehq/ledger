package controllers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/internal"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

func TestGetBalancesAggregated(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {
		store, _, err := storageDriver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)

		_, err = store.Initialize(context.Background())
		require.NoError(t, err)

		require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
			"world": {
				"USD": core.NewEmptyVolumes().WithOutput(core.NewMonetaryInt(250)),
			},
			"alice": {
				"USD": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(150)),
			},
			"bob": {
				"USD": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(100)),
			},
		}))

		t.Run("all", func(t *testing.T) {
			rsp := internal.GetBalancesAggregated(api, url.Values{})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

			resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)
			require.Equal(t, ok, true)
			require.Equal(t, core.AssetsBalances{"USD": core.NewMonetaryInt(0)}, resp)
		})

		t.Run("filter by address", func(t *testing.T) {
			rsp := internal.GetBalancesAggregated(api, url.Values{"address": []string{"world"}})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

			resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)
			require.Equal(t, true, ok)
			require.Equal(t, core.AssetsBalances{"USD": core.NewMonetaryInt(-250)}, resp)
		})

		t.Run("filter by address no result", func(t *testing.T) {
			rsp := internal.GetBalancesAggregated(api, url.Values{"address": []string{"XXX"}})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

			resp, ok := internal.DecodeSingleResponse[core.AssetsBalances](t, rsp.Body)
			require.Equal(t, ok, true)
			require.Equal(t, core.AssetsBalances{}, resp)
		})
	})
}

func TestGetBalances(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {
		store, _, err := storageDriver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)

		_, err = store.Initialize(context.Background())
		require.NoError(t, err)

		require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
			"world": {
				"USD": core.NewEmptyVolumes().WithOutput(core.NewMonetaryInt(250)),
				"CAD": core.NewEmptyVolumes().WithOutput(core.NewMonetaryInt(200)),
				"EUR": core.NewEmptyVolumes().WithOutput(core.NewMonetaryInt(400)),
			},
			"alice": {
				"USD": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(150)),
				"CAD": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(200)),
				"EUR": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(400)),
			},
			"bob": {
				"USD": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(100)),
			},
		}))

		to := ledgerstore.BalancesPaginationToken{}
		raw, err := json.Marshal(to)
		require.NoError(t, err)

		t.Run("valid empty "+controllers.QueryKeyCursor, func(t *testing.T) {
			rsp := internal.GetBalances(api, url.Values{
				controllers.QueryKeyCursor: []string{base64.RawURLEncoding.EncodeToString(raw)},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
		})

		t.Run(fmt.Sprintf("valid empty %s with any other param is forbidden", controllers.QueryKeyCursor), func(t *testing.T) {
			rsp := internal.GetBalances(api, url.Values{
				controllers.QueryKeyCursor: []string{base64.RawURLEncoding.EncodeToString(raw)},
				"after":                    []string{"bob"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())
		})

		t.Run(fmt.Sprintf("invalid %s", controllers.QueryKeyCursor), func(t *testing.T) {
			rsp := internal.GetBalances(api, url.Values{
				controllers.QueryKeyCursor: []string{"invalid"},
			})

			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())
			require.Contains(t, rsp.Body.String(),
				fmt.Sprintf(`"invalid '%s' query param"`, controllers.QueryKeyCursor))
		})

		t.Run("all", func(t *testing.T) {
			rsp := internal.GetBalances(api, url.Values{})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

			resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
			require.Equal(t, []core.AccountsBalances{
				{"world": core.AssetsBalances{"USD": core.NewMonetaryInt(-250), "EUR": core.NewMonetaryInt(-400), "CAD": core.NewMonetaryInt(-200)}},
				{"bob": core.AssetsBalances{"USD": core.NewMonetaryInt(100)}},
				{"alice": core.AssetsBalances{"USD": core.NewMonetaryInt(150), "EUR": core.NewMonetaryInt(400), "CAD": core.NewMonetaryInt(200)}},
			}, resp.Data)
		})

		t.Run("after address", func(t *testing.T) {
			rsp := internal.GetBalances(api, url.Values{"after": []string{"bob"}})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

			resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
			require.Equal(t, []core.AccountsBalances{
				{"alice": core.AssetsBalances{"USD": core.NewMonetaryInt(150), "EUR": core.NewMonetaryInt(400), "CAD": core.NewMonetaryInt(200)}},
			}, resp.Data)
		})

		t.Run("filter by address", func(t *testing.T) {
			rsp := internal.GetBalances(api, url.Values{"address": []string{"world"}})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

			resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
			require.Equal(t, []core.AccountsBalances{
				{"world": core.AssetsBalances{"USD": core.NewMonetaryInt(-250), "EUR": core.NewMonetaryInt(-400), "CAD": core.NewMonetaryInt(-200)}},
			}, resp.Data)
		})

		t.Run("filter by address no results", func(t *testing.T) {
			rsp := internal.GetBalances(api, url.Values{"address": []string{"TEST"}})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

			resp := internal.DecodeCursorResponse[core.AccountsBalances](t, rsp.Body)
			require.Equal(t, []core.AccountsBalances{}, resp.Data)
		})
	})
}
