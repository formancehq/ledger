# \BalancesApi

All URIs are relative to *https://.o.numary.cloud/ledger*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GetBalances**](BalancesApi.md#GetBalances) | **Get** /{ledger}/balances | Get the balances from a ledger&#39;s account
[**GetBalancesAggregated**](BalancesApi.md#GetBalancesAggregated) | **Get** /{ledger}/aggregate/balances | Get the aggregated balances from selected accounts



## GetBalances

> GetBalances200Response GetBalances(ctx, ledger).Address(address).After(after).PaginationToken(paginationToken).Execute()

Get the balances from a ledger's account

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "github.com/numary/numary-go"
)

func main() {
    ledger := "ledger001" // string | Name of the ledger.
    address := "users:001" // string | Filter balances involving given account, either as source or destination. (optional)
    after := "users:003" // string | Pagination cursor, will return accounts after given address, in descending order. (optional)
    paginationToken := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests.  Set to the value of next for the next page of results.  Set to the value of previous for the previous page of results. (optional)

    configuration := client.NewConfiguration()
    api_client := client.NewAPIClient(configuration)
    resp, r, err := api_client.BalancesApi.GetBalances(context.Background(), ledger).Address(address).After(after).PaginationToken(paginationToken).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `BalancesApi.GetBalances``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetBalances`: GetBalances200Response
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
 **paginationToken** | **string** | Parameter used in pagination requests.  Set to the value of next for the next page of results.  Set to the value of previous for the previous page of results. | 

### Return type

[**GetBalances200Response**](GetBalances200Response.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetBalancesAggregated

> GetBalancesAggregated200Response GetBalancesAggregated(ctx, ledger).Address(address).Execute()

Get the aggregated balances from selected accounts

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "github.com/numary/numary-go"
)

func main() {
    ledger := "ledger001" // string | Name of the ledger.
    address := "users:001" // string | Filter balances involving given account, either as source or destination. (optional)

    configuration := client.NewConfiguration()
    api_client := client.NewAPIClient(configuration)
    resp, r, err := api_client.BalancesApi.GetBalancesAggregated(context.Background(), ledger).Address(address).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `BalancesApi.GetBalancesAggregated``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetBalancesAggregated`: GetBalancesAggregated200Response
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

[**GetBalancesAggregated200Response**](GetBalancesAggregated200Response.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

