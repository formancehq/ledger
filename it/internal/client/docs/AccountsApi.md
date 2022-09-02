# \AccountsApi

All URIs are relative to *https://.o.numary.cloud/ledger*

Method | HTTP request | Description
------------- | ------------- | -------------
[**AddMetadataToAccount**](AccountsApi.md#AddMetadataToAccount) | **Post** /{ledger}/accounts/{address}/metadata | Add metadata to an account.
[**CountAccounts**](AccountsApi.md#CountAccounts) | **Head** /{ledger}/accounts | Count the accounts from a ledger.
[**GetAccount**](AccountsApi.md#GetAccount) | **Get** /{ledger}/accounts/{address} | Get account by its address.
[**ListAccounts**](AccountsApi.md#ListAccounts) | **Get** /{ledger}/accounts | List accounts from a ledger.



## AddMetadataToAccount

> AddMetadataToAccount(ctx, ledger, address).RequestBody(requestBody).Execute()

Add metadata to an account.

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
    address := "users:001" // string | Exact address of the account. It must match the following regular expressions pattern: ``` ^\\w+(:\\w+)*$ ``` 
    requestBody := map[string]interface{}{"key": interface{}(123)} // map[string]interface{} | metadata

    configuration := client.NewConfiguration()
    api_client := client.NewAPIClient(configuration)
    resp, r, err := api_client.AccountsApi.AddMetadataToAccount(context.Background(), ledger, address).RequestBody(requestBody).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `AccountsApi.AddMetadataToAccount``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 
**address** | **string** | Exact address of the account. It must match the following regular expressions pattern: &#x60;&#x60;&#x60; ^\\w+(:\\w+)*$ &#x60;&#x60;&#x60;  | 

### Other Parameters

Other parameters are passed through a pointer to a apiAddMetadataToAccountRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


 **requestBody** | **map[string]interface{}** | metadata | 

### Return type

 (empty response body)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## CountAccounts

> CountAccounts(ctx, ledger).Address(address).Metadata(metadata).Execute()

Count the accounts from a ledger.

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
    address := "users:.+" // string | Filter accounts by address pattern (regular expression placed between ^ and $). (optional)
    metadata := map[string]interface{}{"key": map[string]interface{}(123)} // map[string]interface{} | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)

    configuration := client.NewConfiguration()
    api_client := client.NewAPIClient(configuration)
    resp, r, err := api_client.AccountsApi.CountAccounts(context.Background(), ledger).Address(address).Metadata(metadata).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `AccountsApi.CountAccounts``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 

### Other Parameters

Other parameters are passed through a pointer to a apiCountAccountsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **address** | **string** | Filter accounts by address pattern (regular expression placed between ^ and $). | 
 **metadata** | [**map[string]interface{}**](map[string]interface{}.md) | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. | 

### Return type

 (empty response body)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetAccount

> GetAccount200Response GetAccount(ctx, ledger, address).Execute()

Get account by its address.

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
    address := "users:001" // string | Exact address of the account. It must match the following regular expressions pattern: ``` ^\\w+(:\\w+)*$ ``` 

    configuration := client.NewConfiguration()
    api_client := client.NewAPIClient(configuration)
    resp, r, err := api_client.AccountsApi.GetAccount(context.Background(), ledger, address).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `AccountsApi.GetAccount``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetAccount`: GetAccount200Response
    fmt.Fprintf(os.Stdout, "Response from `AccountsApi.GetAccount`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 
**address** | **string** | Exact address of the account. It must match the following regular expressions pattern: &#x60;&#x60;&#x60; ^\\w+(:\\w+)*$ &#x60;&#x60;&#x60;  | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetAccountRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------



### Return type

[**GetAccount200Response**](GetAccount200Response.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListAccounts

> ListAccounts200Response ListAccounts(ctx, ledger).PageSize(pageSize).After(after).Address(address).Metadata(metadata).Balance(balance).BalanceOperator(balanceOperator).PaginationToken(paginationToken).Execute()

List accounts from a ledger.



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
    pageSize := int32(100) // int32 | The maximum number of results to return per page (optional) (default to 15)
    after := "users:003" // string | Pagination cursor, will return accounts after given address, in descending order. (optional)
    address := "users:.+" // string | Filter accounts by address pattern (regular expression placed between ^ and $). (optional)
    metadata := map[string]interface{}{"key": map[string]interface{}(123)} // map[string]interface{} | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
    balance := int64(2400) // int64 | Filter accounts by their balance (default operator is gte) (optional)
    balanceOperator := "gte" // string | Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, or equal (optional)
    paginationToken := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results.  Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  (optional)

    configuration := client.NewConfiguration()
    api_client := client.NewAPIClient(configuration)
    resp, r, err := api_client.AccountsApi.ListAccounts(context.Background(), ledger).PageSize(pageSize).After(after).Address(address).Metadata(metadata).Balance(balance).BalanceOperator(balanceOperator).PaginationToken(paginationToken).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `AccountsApi.ListAccounts``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListAccounts`: ListAccounts200Response
    fmt.Fprintf(os.Stdout, "Response from `AccountsApi.ListAccounts`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 

### Other Parameters

Other parameters are passed through a pointer to a apiListAccountsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **pageSize** | **int32** | The maximum number of results to return per page | [default to 15]
 **after** | **string** | Pagination cursor, will return accounts after given address, in descending order. | 
 **address** | **string** | Filter accounts by address pattern (regular expression placed between ^ and $). | 
 **metadata** | [**map[string]interface{}**](map[string]interface{}.md) | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. | 
 **balance** | **int64** | Filter accounts by their balance (default operator is gte) | 
 **balanceOperator** | **string** | Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, or equal | 
 **paginationToken** | **string** | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results.  Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  | 

### Return type

[**ListAccounts200Response**](ListAccounts200Response.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

