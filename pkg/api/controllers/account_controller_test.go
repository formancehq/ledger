package controllers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/internal"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
)

func TestGetAccounts(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {
		store, _, err := storageDriver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)
		_, err = store.Initialize(context.Background())
		require.NoError(t, err)
		require.NoError(t, store.EnsureAccountExists(context.Background(), "world"))
		require.NoError(t, store.EnsureAccountExists(context.Background(), "alice"))
		require.NoError(t, store.EnsureAccountExists(context.Background(), "bob"))
		meta := core.Metadata{
			"roles":     "admin",
			"accountId": float64(3),
			"enabled":   "true",
			"a": map[string]any{
				"nested": map[string]any{
					"key": "hello",
				},
			},
		}
		require.NoError(t, store.UpdateAccountMetadata(context.Background(), "bob", meta))
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

		rsp := internal.CountAccounts(api, url.Values{})
		require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
		require.Equal(t, "3", rsp.Header().Get("Count"))

		t.Run("all", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			// 3 accounts: world, bob, alice
			require.Len(t, cursor.Data, 3)
			require.Equal(t, []core.Account{
				{Address: "world", Metadata: core.Metadata{}},
				{Address: "bob", Metadata: meta},
				{Address: "alice", Metadata: core.Metadata{}},
			}, cursor.Data)
		})

		t.Run("meta roles", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"metadata[roles]": []string{"admin"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			// 1 accounts: bob
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "bob", string(cursor.Data[0].Address))
		})

		t.Run("meta accountId", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"metadata[accountId]": []string{"3"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			// 1 accounts: bob
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "bob", string(cursor.Data[0].Address))
		})

		t.Run("meta enabled", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"metadata[enabled]": []string{"true"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			// 1 accounts: bob
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "bob", string(cursor.Data[0].Address))
		})

		t.Run("meta nested", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"metadata[a.nested.key]": []string{"hello"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			// 1 accounts: bob
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "bob", string(cursor.Data[0].Address))
		})

		t.Run("meta unknown", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"metadata[unknown]": []string{"key"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 0)
		})

		t.Run("after", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"after": []string{"bob"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			// 1 accounts: alice
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "alice", string(cursor.Data[0].Address))
		})

		t.Run("address", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"address": []string{"b.b"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			// 1 accounts: bob
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "bob", string(cursor.Data[0].Address))
		})

		to := ledgerstore.AccountsPaginationToken{}
		raw, err := json.Marshal(to)
		require.NoError(t, err)

		t.Run(fmt.Sprintf("valid empty %s", controllers.QueryKeyCursor), func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				controllers.QueryKeyCursor: []string{base64.RawURLEncoding.EncodeToString(raw)},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
		})

		t.Run(fmt.Sprintf("valid empty %s with any other param is forbidden", controllers.QueryKeyCursor), func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				controllers.QueryKeyCursor: []string{base64.RawURLEncoding.EncodeToString(raw)},
				"after":                    []string{"bob"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: fmt.Sprintf("no other query params can be set with '%s'", controllers.QueryKeyCursor),
			}, err)
		})

		t.Run(fmt.Sprintf("invalid %s", controllers.QueryKeyCursor), func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				controllers.QueryKeyCursor: []string{"invalid"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: fmt.Sprintf("invalid '%s' query param", controllers.QueryKeyCursor),
			}, err)
		})

		t.Run(fmt.Sprintf("invalid %s not base64", controllers.QueryKeyCursor), func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				controllers.QueryKeyCursor: []string{"\n*@"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: fmt.Sprintf("invalid '%s' query param", controllers.QueryKeyCursor),
			}, err)
		})

		t.Run("filter by balance >= 50 with default operator", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"balance": []string{"50"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 2)
			require.Equal(t, "bob", string(cursor.Data[0].Address))
			require.Equal(t, "alice", string(cursor.Data[1].Address))
		})

		t.Run("filter by balance >= 120 with default operator", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"balance": []string{"120"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "alice", string(cursor.Data[0].Address))
		})

		t.Run("filter by balance >= 50", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"balance":                           []string{"50"},
				controllers.QueryKeyBalanceOperator: []string{"gte"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 2)
			require.Equal(t, "bob", string(cursor.Data[0].Address))
			require.Equal(t, "alice", string(cursor.Data[1].Address))
		})

		t.Run("filter by balance >= 120", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"balance":                           []string{"120"},
				controllers.QueryKeyBalanceOperator: []string{"gte"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "alice", string(cursor.Data[0].Address))
		})

		t.Run("filter by balance > 120", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"balance":                           []string{"120"},
				controllers.QueryKeyBalanceOperator: []string{"gt"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "alice", string(cursor.Data[0].Address))
		})

		t.Run("filter by balance < 0", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"balance":                           []string{"0"},
				controllers.QueryKeyBalanceOperator: []string{"lt"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "world", string(cursor.Data[0].Address))
		})

		t.Run("filter by balance < 100", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"balance":                           []string{"100"},
				controllers.QueryKeyBalanceOperator: []string{"lt"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "world", string(cursor.Data[0].Address))
		})

		t.Run("filter by balance <= 100", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"balance":                           []string{"100"},
				controllers.QueryKeyBalanceOperator: []string{"lte"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 2)
			require.Equal(t, "world", string(cursor.Data[0].Address))
			require.Equal(t, "bob", string(cursor.Data[1].Address))
		})

		t.Run("filter by balance = 100", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"balance":                           []string{"100"},
				controllers.QueryKeyBalanceOperator: []string{"e"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 1)
			require.Equal(t, "bob", string(cursor.Data[0].Address))
		})

		// test filter by balance != 100
		t.Run("filter by balance != 100", func(t *testing.T) {
			rsp = internal.GetAccounts(api, url.Values{
				"balance":                           []string{"100"},
				controllers.QueryKeyBalanceOperator: []string{"ne"},
			})
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
			require.Len(t, cursor.Data, 2)
			require.Equal(t, "world", string(cursor.Data[0].Address))
			require.Equal(t, "alice", string(cursor.Data[1].Address))
		})

		t.Run("invalid balance", func(t *testing.T) {
			rsp := internal.GetAccounts(api, url.Values{
				"balance": []string{"toto"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid parameter 'balance', should be a number",
			}, err)
		})

		t.Run("invalid balance operator", func(t *testing.T) {
			rsp := internal.GetAccounts(api, url.Values{
				"balance":                           []string{"100"},
				controllers.QueryKeyBalanceOperator: []string{"toto"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: controllers.ErrInvalidBalanceOperator.Error(),
			}, err)
		})
	})
}

func TestGetAccountsWithPageSize(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, driver storage.Driver) {
		store := internal.GetLedgerStore(t, driver, context.Background())

		_, err := store.Initialize(context.Background())
		require.NoError(t, err)

		for i := 0; i < 3*controllers.MaxPageSize; i++ {
			require.NoError(t, store.UpdateAccountMetadata(context.Background(), fmt.Sprintf("accounts:%06d", i), core.Metadata{
				"foo": []byte("{}"),
			}))
		}

		t.Run("invalid page size", func(t *testing.T) {
			rsp := internal.GetAccounts(api, url.Values{
				controllers.QueryKeyPageSize: []string{"nan"},
			})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: controllers.ErrInvalidPageSize.Error(),
			}, err)
		})
		t.Run("page size over maximum", func(t *testing.T) {
			httpResponse := internal.GetAccounts(api, url.Values{
				controllers.QueryKeyPageSize: []string{fmt.Sprintf("%d", 2*controllers.MaxPageSize)},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor := internal.DecodeCursorResponse[core.Account](t, httpResponse.Body)
			require.Len(t, cursor.Data, controllers.MaxPageSize)
			require.Equal(t, cursor.PageSize, controllers.MaxPageSize)
			require.NotEmpty(t, cursor.Next)
			require.True(t, cursor.HasMore)
		})
		t.Run("with page size greater than max count", func(t *testing.T) {
			httpResponse := internal.GetAccounts(api, url.Values{
				controllers.QueryKeyPageSize: []string{fmt.Sprintf("%d", controllers.MaxPageSize)},
				"after":                      []string{fmt.Sprintf("accounts:%06d", controllers.MaxPageSize-100)},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor := internal.DecodeCursorResponse[core.Account](t, httpResponse.Body)
			require.Len(t, cursor.Data, controllers.MaxPageSize-100)
			require.Equal(t, controllers.MaxPageSize, cursor.PageSize)
			require.Empty(t, cursor.Next)
			require.False(t, cursor.HasMore)
		})
		t.Run("with page size lower than max count", func(t *testing.T) {
			httpResponse := internal.GetAccounts(api, url.Values{
				controllers.QueryKeyPageSize: []string{fmt.Sprintf("%d", controllers.MaxPageSize/10)},
			})
			require.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

			cursor := internal.DecodeCursorResponse[core.Account](t, httpResponse.Body)
			require.Len(t, cursor.Data, controllers.MaxPageSize/10)
			require.Equal(t, cursor.PageSize, controllers.MaxPageSize/10)
			require.NotEmpty(t, cursor.Next)
			require.True(t, cursor.HasMore)
		})
	})
}

func TestGetAccount(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {
		store, _, err := storageDriver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)

		_, err = store.Initialize(context.Background())
		require.NoError(t, err)

		require.NoError(t, store.UpdateAccountMetadata(context.Background(), "alice", core.Metadata{
			"foo": json.RawMessage(`"bar"`),
		}))
		require.NoError(t, store.UpdateVolumes(context.Background(), core.AccountsAssetsVolumes{
			"alice": {
				"USD": core.NewEmptyVolumes().WithInput(core.NewMonetaryInt(100)),
			},
		}))

		t.Run("valid address", func(t *testing.T) {
			rsp := internal.GetAccount(api, "alice")
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			resp, _ := internal.DecodeSingleResponse[core.AccountWithVolumes](t, rsp.Body)

			require.EqualValues(t, core.AccountWithVolumes{
				Account: core.Account{
					Address: "alice",
					Metadata: core.Metadata{
						"foo": "bar",
					},
				},
				Balances: core.AssetsBalances{
					"USD": core.NewMonetaryInt(100),
				},
				Volumes: core.AssetsVolumes{
					"USD": {
						Input:  core.NewMonetaryInt(100),
						Output: core.NewMonetaryInt(0),
					},
				},
			}, resp)
		})

		t.Run("unknown address", func(t *testing.T) {
			rsp := internal.GetAccount(api, "bob")
			require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
			resp, _ := internal.DecodeSingleResponse[core.AccountWithVolumes](t, rsp.Body)
			require.EqualValues(t, core.AccountWithVolumes{
				Account: core.Account{
					Address:  "bob",
					Metadata: core.Metadata{},
				},
				Balances: core.AssetsBalances{},
				Volumes:  core.AssetsVolumes{},
			}, resp)
		})

		t.Run("invalid address format", func(t *testing.T) {
			rsp := internal.GetAccount(api, "accounts::alice")
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid account address format",
			}, err)
		})
	})
}

func TestPostAccountMetadata(t *testing.T) {
	internal.RunTest(t, func(api chi.Router, storageDriver storage.Driver) {
		store, _, err := storageDriver.GetLedgerStore(context.Background(), internal.TestingLedger, true)
		require.NoError(t, err)

		_, err = store.Initialize(context.Background())
		require.NoError(t, err)

		require.NoError(t, store.EnsureAccountExists(context.Background(), "alice"))

		t.Run("valid request", func(t *testing.T) {
			rsp := internal.PostAccountMetadata(t, api, "alice",
				core.Metadata{
					"foo": json.RawMessage(`"bar"`),
				})
			require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode, rsp.Body.String())
		})

		t.Run("unknown account should succeed", func(t *testing.T) {
			rsp := internal.PostAccountMetadata(t, api, "bob",
				core.Metadata{
					"foo": json.RawMessage(`"bar"`),
				})
			require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode, rsp.Body.String())
		})

		t.Run("invalid address format", func(t *testing.T) {
			rsp := internal.PostAccountMetadata(t, api, "accounts::alice", core.Metadata{})
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid account address format",
			}, err)
		})

		t.Run("invalid metadata format", func(t *testing.T) {
			rsp := internal.NewRequestOnLedger(t, api, "/accounts/alice/metadata", "invalid")
			require.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

			err := sharedapi.ErrorResponse{}
			internal.Decode(t, rsp.Body, &err)
			require.EqualValues(t, sharedapi.ErrorResponse{
				ErrorCode:    apierrors.ErrValidation,
				ErrorMessage: "invalid metadata format",
			}, err)
		})
	})
}
