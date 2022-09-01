package controllers_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/internal"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestGetAccounts(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(150),
							Asset:       "USD",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "bob",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

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
				rsp = internal.PostAccountMetadata(t, api, "bob", meta)
				require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

				rsp = internal.CountAccounts(api, url.Values{})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)
				require.Equal(t, "3", rsp.Header().Get("Count"))

				t.Run("all", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 3 accounts: world, bob, alice
					assert.Len(t, cursor.Data, 3)
					assert.Equal(t, []core.Account{
						{Address: "world", Metadata: core.Metadata{}},
						{Address: "bob", Metadata: meta},
						{Address: "alice", Metadata: core.Metadata{}},
					}, cursor.Data)
				})

				t.Run("meta roles", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"metadata[roles]": []string{"admin"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: bob
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				t.Run("meta accountId", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"metadata[accountId]": []string{"3"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: bob
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				t.Run("meta enabled", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"metadata[enabled]": []string{"true"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: bob
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				t.Run("meta nested", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"metadata[a.nested.key]": []string{"hello"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: bob
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				t.Run("meta unknown", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"metadata[unknown]": []string{"key"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 0)
				})

				t.Run("after", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"after": []string{"bob"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: alice
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "alice")
				})

				t.Run("address", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"address": []string{"b.b"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					// 1 accounts: bob
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				to := sqlstorage.AccPaginationToken{}
				raw, err := json.Marshal(to)
				require.NoError(t, err)
				t.Run("valid empty pagination_token", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"pagination_token": []string{base64.RawURLEncoding.EncodeToString(raw)},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run("valid empty pagination_token with any other param is forbidden", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"pagination_token": []string{base64.RawURLEncoding.EncodeToString(raw)},
						"after":            []string{"bob"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					assert.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    controllers.ErrValidation,
						ErrorMessage: "no other query params can be set with 'pagination_token'",
					}, err)
				})

				t.Run("invalid pagination_token", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"pagination_token": []string{"invalid"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					assert.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    controllers.ErrValidation,
						ErrorMessage: "invalid query value 'pagination_token'",
					}, err)
				})

				t.Run("invalid pagination_token not base64", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"pagination_token": []string{"\n*@"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					assert.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    controllers.ErrValidation,
						ErrorMessage: "invalid query value 'pagination_token'",
					}, err)
				})

				t.Run("filter by balance >= 50 with default operator", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"balance": []string{"50"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 2)
					assert.Equal(t, cursor.Data[0].Address, "bob")
					assert.Equal(t, cursor.Data[1].Address, "alice")
				})

				t.Run("filter by balance >= 120 with default operator", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"balance": []string{"120"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "alice")
				})

				t.Run("filter by balance >= 50", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"balance":          []string{"50"},
						"balance_operator": []string{"gte"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 2)
					assert.Equal(t, cursor.Data[0].Address, "bob")
					assert.Equal(t, cursor.Data[1].Address, "alice")
				})

				t.Run("filter by balance >= 120", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"balance":          []string{"120"},
						"balance_operator": []string{"gte"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "alice")
				})

				t.Run("filter by balance > 120", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"balance":          []string{"120"},
						"balance_operator": []string{"gt"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "alice")
				})

				t.Run("filter by balance < 0", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"balance":          []string{"0"},
						"balance_operator": []string{"lt"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "world")
				})

				t.Run("filter by balance < 100", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"balance":          []string{"100"},
						"balance_operator": []string{"lt"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "world")
				})

				t.Run("filter by balance <= 100", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"balance":          []string{"100"},
						"balance_operator": []string{"lte"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 2)
					assert.Equal(t, cursor.Data[0].Address, "world")
					assert.Equal(t, cursor.Data[1].Address, "bob")
				})

				t.Run("filter by balance = 100", func(t *testing.T) {
					rsp = internal.GetAccounts(api, url.Values{
						"balance":          []string{"100"},
						"balance_operator": []string{"e"},
					})
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					cursor := internal.DecodeCursorResponse[core.Account](t, rsp.Body)
					assert.Len(t, cursor.Data, 1)
					assert.Equal(t, cursor.Data[0].Address, "bob")
				})

				t.Run("invalid balance", func(t *testing.T) {
					rsp := internal.GetAccounts(api, url.Values{
						"balance": []string{"toto"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					assert.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    controllers.ErrValidation,
						ErrorMessage: "invalid parameter 'balance', should be a number",
					}, err)
				})

				t.Run("invalid balance_operator", func(t *testing.T) {
					rsp := internal.GetAccounts(api, url.Values{
						"balance":          []string{"100"},
						"balance_operator": []string{"toto"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode)

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					assert.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    controllers.ErrValidation,
						ErrorMessage: "invalid parameter 'balance_operator', should be one of 'e, gt, gte, lt, lte'",
					}, err)
				})

				return nil
			},
		})
	}))
}

func TestGetAccountsWithPageSize(t *testing.T) {
	now := time.Now()
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API, driver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				store := internal.GetStore(t, driver, context.Background())

				for i := 0; i < 3*controllers.MaxPageSize; i++ {
					require.NoError(t, store.UpdateAccountMetadata(ctx, fmt.Sprintf("accounts:%06d", i), core.Metadata{
						"foo": []byte("{}"),
					}, now))
				}

				t.Run("invalid page size", func(t *testing.T) {
					rsp := internal.GetAccounts(api, url.Values{
						"page_size": []string{"nan"},
					})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					assert.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    controllers.ErrValidation,
						ErrorMessage: controllers.ErrInvalidPageSize.Error(),
					}, err)
				})
				t.Run("page size over maximum", func(t *testing.T) {
					httpResponse := internal.GetAccounts(api, url.Values{
						"page_size": []string{fmt.Sprintf("%d", 2*controllers.MaxPageSize)},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor := internal.DecodeCursorResponse[core.Account](t, httpResponse.Body)
					assert.Len(t, cursor.Data, controllers.MaxPageSize)
					assert.Equal(t, cursor.PageSize, controllers.MaxPageSize)
					assert.NotEmpty(t, cursor.Next)
					assert.True(t, cursor.HasMore)
				})
				t.Run("with page size greater than max count", func(t *testing.T) {
					httpResponse := internal.GetAccounts(api, url.Values{
						"page_size": []string{fmt.Sprintf("%d", controllers.MaxPageSize)},
						"after":     []string{fmt.Sprintf("accounts:%06d", controllers.MaxPageSize-100)},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor := internal.DecodeCursorResponse[core.Account](t, httpResponse.Body)
					assert.Len(t, cursor.Data, controllers.MaxPageSize-100)
					assert.Equal(t, controllers.MaxPageSize, cursor.PageSize)
					assert.Empty(t, cursor.Next)
					assert.False(t, cursor.HasMore)
				})
				t.Run("with page size lower than max count", func(t *testing.T) {
					httpResponse := internal.GetAccounts(api, url.Values{
						"page_size": []string{fmt.Sprintf("%d", controllers.MaxPageSize/10)},
					})
					assert.Equal(t, http.StatusOK, httpResponse.Result().StatusCode, httpResponse.Body.String())

					cursor := internal.DecodeCursorResponse[core.Account](t, httpResponse.Body)
					assert.Len(t, cursor.Data, controllers.MaxPageSize/10)
					assert.Equal(t, cursor.PageSize, controllers.MaxPageSize/10)
					assert.NotEmpty(t, cursor.Next)
					assert.True(t, cursor.HasMore)
				})

				return nil
			},
		})
	}))
}

func TestGetAccount(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				rsp = internal.PostAccountMetadata(t, api, "alice",
					core.Metadata{
						"foo": json.RawMessage(`"bar"`),
					})
				require.Equal(t, http.StatusNoContent, rsp.Result().StatusCode)

				t.Run("valid address", func(t *testing.T) {
					rsp = internal.GetAccount(api, "alice")
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					resp, _ := internal.DecodeSingleResponse[core.AccountWithVolumes](t, rsp.Body)

					assert.EqualValues(t, core.AccountWithVolumes{
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
					rsp = internal.GetAccount(api, "bob")
					assert.Equal(t, http.StatusOK, rsp.Result().StatusCode)
					resp, _ := internal.DecodeSingleResponse[core.AccountWithVolumes](t, rsp.Body)
					assert.EqualValues(t, core.AccountWithVolumes{
						Account: core.Account{
							Address:  "bob",
							Metadata: core.Metadata{},
						},
						Balances: core.AssetsBalances{},
						Volumes:  core.AssetsVolumes{},
					}, resp)
				})

				t.Run("invalid address format", func(t *testing.T) {
					rsp = internal.GetAccount(api, "accounts::alice")
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					assert.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    controllers.ErrValidation,
						ErrorMessage: "invalid account address format",
					}, err)
				})

				return nil
			},
		})
	}))
}

func TestPostAccountMetadata(t *testing.T) {
	internal.RunTest(t, fx.Invoke(func(lc fx.Lifecycle, api *api.API) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				rsp := internal.PostTransaction(t, api, core.TransactionData{
					Postings: core.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      core.NewMonetaryInt(100),
							Asset:       "USD",
						},
					},
				})
				require.Equal(t, http.StatusOK, rsp.Result().StatusCode)

				t.Run("valid request", func(t *testing.T) {
					rsp = internal.PostAccountMetadata(t, api, "alice",
						core.Metadata{
							"foo": json.RawMessage(`"bar"`),
						})
					assert.Equal(t, http.StatusNoContent, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run("unknown account should succeed", func(t *testing.T) {
					rsp = internal.PostAccountMetadata(t, api, "bob",
						core.Metadata{
							"foo": json.RawMessage(`"bar"`),
						})
					assert.Equal(t, http.StatusNoContent, rsp.Result().StatusCode, rsp.Body.String())
				})

				t.Run("invalid address format", func(t *testing.T) {
					rsp = internal.PostAccountMetadata(t, api, "accounts::alice", core.Metadata{})
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					assert.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    controllers.ErrValidation,
						ErrorMessage: "invalid account address format",
					}, err)
				})

				t.Run("invalid metadata format", func(t *testing.T) {
					rsp = internal.NewRequestOnLedger(t, api, "/accounts/alice/metadata", "invalid")
					assert.Equal(t, http.StatusBadRequest, rsp.Result().StatusCode, rsp.Body.String())

					err := sharedapi.ErrorResponse{}
					internal.Decode(t, rsp.Body, &err)
					assert.EqualValues(t, sharedapi.ErrorResponse{
						ErrorCode:    controllers.ErrValidation,
						ErrorMessage: "invalid metadata format",
					}, err)
				})

				return nil
			},
		})
	}))
}
