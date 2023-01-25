# \WalletsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ConfirmHold**](WalletsApi.md#ConfirmHold) | **Post** /api/wallets/holds/{hold_id}/confirm | Confirm a hold
[**CreateBalance**](WalletsApi.md#CreateBalance) | **Post** /api/wallets/wallets/{id}/balances | Create a balance
[**CreateWallet**](WalletsApi.md#CreateWallet) | **Post** /api/wallets/wallets | Create a new wallet
[**CreditWallet**](WalletsApi.md#CreditWallet) | **Post** /api/wallets/wallets/{id}/credit | Credit a wallet
[**DebitWallet**](WalletsApi.md#DebitWallet) | **Post** /api/wallets/wallets/{id}/debit | Debit a wallet
[**GetBalance**](WalletsApi.md#GetBalance) | **Get** /api/wallets/wallets/{id}/balances/{balanceName} | Get detailed balance
[**GetHold**](WalletsApi.md#GetHold) | **Get** /api/wallets/holds/{holdID} | Get a hold
[**GetHolds**](WalletsApi.md#GetHolds) | **Get** /api/wallets/holds | Get all holds for a wallet
[**GetTransactions**](WalletsApi.md#GetTransactions) | **Get** /api/wallets/transactions | 
[**GetWallet**](WalletsApi.md#GetWallet) | **Get** /api/wallets/wallets/{id} | Get a wallet
[**ListBalances**](WalletsApi.md#ListBalances) | **Get** /api/wallets/wallets/{id}/balances | List balances of a wallet
[**ListWallets**](WalletsApi.md#ListWallets) | **Get** /api/wallets/wallets | List all wallets
[**UpdateWallet**](WalletsApi.md#UpdateWallet) | **Patch** /api/wallets/wallets/{id} | Update a wallet
[**VoidHold**](WalletsApi.md#VoidHold) | **Post** /api/wallets/holds/{hold_id}/void | Cancel a hold
[**WalletsgetServerInfo**](WalletsApi.md#WalletsgetServerInfo) | **Get** /api/wallets/_info | Get server info



## ConfirmHold

> ConfirmHold(ctx, holdId).ConfirmHoldRequest(confirmHoldRequest).Execute()

Confirm a hold

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    holdId := "holdId_example" // string | 
    confirmHoldRequest := *client.NewConfirmHoldRequest() // ConfirmHoldRequest |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.ConfirmHold(context.Background(), holdId).ConfirmHoldRequest(confirmHoldRequest).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.ConfirmHold``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**holdId** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiConfirmHoldRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **confirmHoldRequest** | [**ConfirmHoldRequest**](ConfirmHoldRequest.md) |  | 

### Return type

 (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## CreateBalance

> CreateBalanceResponse CreateBalance(ctx, id).Body(body).Execute()

Create a balance

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    id := "id_example" // string | 
    body := Balance(987) // Balance |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.CreateBalance(context.Background(), id).Body(body).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.CreateBalance``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `CreateBalance`: CreateBalanceResponse
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.CreateBalance`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiCreateBalanceRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **body** | **Balance** |  | 

### Return type

[**CreateBalanceResponse**](CreateBalanceResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## CreateWallet

> CreateWalletResponse CreateWallet(ctx).CreateWalletRequest(createWalletRequest).Execute()

Create a new wallet

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    createWalletRequest := *client.NewCreateWalletRequest("Name_example") // CreateWalletRequest |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.CreateWallet(context.Background()).CreateWalletRequest(createWalletRequest).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.CreateWallet``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `CreateWallet`: CreateWalletResponse
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.CreateWallet`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiCreateWalletRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **createWalletRequest** | [**CreateWalletRequest**](CreateWalletRequest.md) |  | 

### Return type

[**CreateWalletResponse**](CreateWalletResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## CreditWallet

> CreditWallet(ctx, id).CreditWalletRequest(creditWalletRequest).Execute()

Credit a wallet

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    id := "id_example" // string | 
    creditWalletRequest := *client.NewCreditWalletRequest(*client.NewMonetary("Asset_example", int64(123)), []client.Subject{*client.NewSubject("Type_example", "Identifier_example")}) // CreditWalletRequest |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.CreditWallet(context.Background(), id).CreditWalletRequest(creditWalletRequest).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.CreditWallet``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiCreditWalletRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **creditWalletRequest** | [**CreditWalletRequest**](CreditWalletRequest.md) |  | 

### Return type

 (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## DebitWallet

> DebitWalletResponse DebitWallet(ctx, id).DebitWalletRequest(debitWalletRequest).Execute()

Debit a wallet

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    id := "id_example" // string | 
    debitWalletRequest := *client.NewDebitWalletRequest(*client.NewMonetary("Asset_example", int64(123))) // DebitWalletRequest |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.DebitWallet(context.Background(), id).DebitWalletRequest(debitWalletRequest).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.DebitWallet``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `DebitWallet`: DebitWalletResponse
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.DebitWallet`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiDebitWalletRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **debitWalletRequest** | [**DebitWalletRequest**](DebitWalletRequest.md) |  | 

### Return type

[**DebitWalletResponse**](DebitWalletResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetBalance

> GetBalanceResponse GetBalance(ctx, id, balanceName).Execute()

Get detailed balance

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    id := "id_example" // string | 
    balanceName := "balanceName_example" // string | 

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.GetBalance(context.Background(), id, balanceName).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.GetBalance``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetBalance`: GetBalanceResponse
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.GetBalance`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** |  | 
**balanceName** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetBalanceRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------



### Return type

[**GetBalanceResponse**](GetBalanceResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetHold

> GetHoldResponse GetHold(ctx, holdID).Execute()

Get a hold

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    holdID := "holdID_example" // string | The hold ID

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.GetHold(context.Background(), holdID).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.GetHold``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetHold`: GetHoldResponse
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.GetHold`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**holdID** | **string** | The hold ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetHoldRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**GetHoldResponse**](GetHoldResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetHolds

> GetHoldsResponse GetHolds(ctx).PageSize(pageSize).WalletID(walletID).Metadata(metadata).Cursor(cursor).Execute()

Get all holds for a wallet

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    pageSize := int32(100) // int32 | The maximum number of results to return per page (optional) (default to 15)
    walletID := "wallet1" // string | The wallet to filter on (optional)
    metadata := map[string]interface{}{"key": map[string]interface{}(123)} // map[string]interface{} | Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
    cursor := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.GetHolds(context.Background()).PageSize(pageSize).WalletID(walletID).Metadata(metadata).Cursor(cursor).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.GetHolds``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetHolds`: GetHoldsResponse
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.GetHolds`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiGetHoldsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pageSize** | **int32** | The maximum number of results to return per page | [default to 15]
 **walletID** | **string** | The wallet to filter on | 
 **metadata** | [**map[string]interface{}**](map[string]interface{}.md) | Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below. | 
 **cursor** | **string** | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  | 

### Return type

[**GetHoldsResponse**](GetHoldsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetTransactions

> GetTransactionsResponse GetTransactions(ctx).PageSize(pageSize).WalletId(walletId).Cursor(cursor).Execute()



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    pageSize := int32(100) // int32 | The maximum number of results to return per page (optional) (default to 15)
    walletId := "wallet1" // string | A wallet ID to filter on (optional)
    cursor := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set.  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.GetTransactions(context.Background()).PageSize(pageSize).WalletId(walletId).Cursor(cursor).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.GetTransactions``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetTransactions`: GetTransactionsResponse
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.GetTransactions`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiGetTransactionsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pageSize** | **int32** | The maximum number of results to return per page | [default to 15]
 **walletId** | **string** | A wallet ID to filter on | 
 **cursor** | **string** | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set.  | 

### Return type

[**GetTransactionsResponse**](GetTransactionsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetWallet

> GetWalletResponse GetWallet(ctx, id).Execute()

Get a wallet

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    id := "id_example" // string | 

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.GetWallet(context.Background(), id).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.GetWallet``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetWallet`: GetWalletResponse
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.GetWallet`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetWalletRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**GetWalletResponse**](GetWalletResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListBalances

> ListBalancesResponse ListBalances(ctx, id).Execute()

List balances of a wallet

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    id := "id_example" // string | 

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.ListBalances(context.Background(), id).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.ListBalances``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListBalances`: ListBalancesResponse
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.ListBalances`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiListBalancesRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**ListBalancesResponse**](ListBalancesResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListWallets

> ListWalletsResponse ListWallets(ctx).Name(name).Metadata(metadata).PageSize(pageSize).Cursor(cursor).Execute()

List all wallets

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    name := "wallet1" // string | Filter on wallet name (optional)
    metadata := map[string]interface{}{"key": map[string]interface{}(123)} // map[string]interface{} | Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
    pageSize := int32(100) // int32 | The maximum number of results to return per page (optional) (default to 15)
    cursor := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.ListWallets(context.Background()).Name(name).Metadata(metadata).PageSize(pageSize).Cursor(cursor).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.ListWallets``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListWallets`: ListWalletsResponse
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.ListWallets`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiListWalletsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | **string** | Filter on wallet name | 
 **metadata** | [**map[string]interface{}**](map[string]interface{}.md) | Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below. | 
 **pageSize** | **int32** | The maximum number of results to return per page | [default to 15]
 **cursor** | **string** | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  | 

### Return type

[**ListWalletsResponse**](ListWalletsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## UpdateWallet

> UpdateWallet(ctx, id).UpdateWalletRequest(updateWalletRequest).Execute()

Update a wallet

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    id := "id_example" // string | 
    updateWalletRequest := *client.NewUpdateWalletRequest() // UpdateWalletRequest |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.UpdateWallet(context.Background(), id).UpdateWalletRequest(updateWalletRequest).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.UpdateWallet``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiUpdateWalletRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **updateWalletRequest** | [**UpdateWalletRequest**](UpdateWalletRequest.md) |  | 

### Return type

 (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## VoidHold

> VoidHold(ctx, holdId).Execute()

Cancel a hold

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    holdId := "holdId_example" // string | 

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.VoidHold(context.Background(), holdId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.VoidHold``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**holdId** | **string** |  | 

### Other Parameters

Other parameters are passed through a pointer to a apiVoidHoldRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

 (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## WalletsgetServerInfo

> ServerInfo WalletsgetServerInfo(ctx).Execute()

Get server info

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WalletsApi.WalletsgetServerInfo(context.Background()).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WalletsApi.WalletsgetServerInfo``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `WalletsgetServerInfo`: ServerInfo
    fmt.Fprintf(os.Stdout, "Response from `WalletsApi.WalletsgetServerInfo`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiWalletsgetServerInfoRequest struct via the builder pattern


### Return type

[**ServerInfo**](ServerInfo.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

