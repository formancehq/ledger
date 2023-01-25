# \BalancesApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GetBalances**](BalancesApi.md#GetBalances) | **Get** /api/ledger/{ledger}/balances | Get the balances from a ledger&#39;s account
[**GetBalancesAggregated**](BalancesApi.md#GetBalancesAggregated) | **Get** /api/ledger/{ledger}/aggregate/balances | Get the aggregated balances from selected accounts



## GetBalances

> BalancesCursorResponse GetBalances(ctx, ledger).Address(address).After(after).Cursor(cursor).PaginationToken(paginationToken).Execute()

Get the balances from a ledger's account

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
    ledger := "ledger001" // string | Name of the ledger.
    address := "users:001" // string | Filter balances involving given account, either as source or destination. (optional)
    after := "users:003" // string | Pagination cursor, will return accounts after given address, in descending order. (optional)
    cursor := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
    paginationToken := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. Deprecated, please use `cursor` instead. (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.BalancesApi.GetBalances(context.Background(), ledger).Address(address).After(after).Cursor(cursor).PaginationToken(paginationToken).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `BalancesApi.GetBalances``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetBalances`: BalancesCursorResponse
    fmt.Fprintf(os.Stdout, "Response from `BalancesApi.GetBalances`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetBalancesRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **address** | **string** | Filter balances involving given account, either as source or destination. | 
 **after** | **string** | Pagination cursor, will return accounts after given address, in descending order. | 
 **cursor** | **string** | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | 
 **paginationToken** | **string** | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. Deprecated, please use &#x60;cursor&#x60; instead. | 

### Return type

[**BalancesCursorResponse**](BalancesCursorResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetBalancesAggregated

> AggregateBalancesResponse GetBalancesAggregated(ctx, ledger).Address(address).Execute()

Get the aggregated balances from selected accounts

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
    ledger := "ledger001" // string | Name of the ledger.
    address := "users:001" // string | Filter balances involving given account, either as source or destination. (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.BalancesApi.GetBalancesAggregated(context.Background(), ledger).Address(address).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `BalancesApi.GetBalancesAggregated``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetBalancesAggregated`: AggregateBalancesResponse
    fmt.Fprintf(os.Stdout, "Response from `BalancesApi.GetBalancesAggregated`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetBalancesAggregatedRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **address** | **string** | Filter balances involving given account, either as source or destination. | 

### Return type

[**AggregateBalancesResponse**](AggregateBalancesResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

