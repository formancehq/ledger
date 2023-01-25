# \AccountsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**AddMetadataToAccount**](AccountsApi.md#AddMetadataToAccount) | **Post** /api/ledger/{ledger}/accounts/{address}/metadata | Add metadata to an account
[**CountAccounts**](AccountsApi.md#CountAccounts) | **Head** /api/ledger/{ledger}/accounts | Count the accounts from a ledger
[**GetAccount**](AccountsApi.md#GetAccount) | **Get** /api/ledger/{ledger}/accounts/{address} | Get account by its address
[**ListAccounts**](AccountsApi.md#ListAccounts) | **Get** /api/ledger/{ledger}/accounts | List accounts from a ledger



## AddMetadataToAccount

> AddMetadataToAccount(ctx, ledger, address).RequestBody(requestBody).Execute()

Add metadata to an account

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
    address := "users:001" // string | Exact address of the account. It must match the following regular expressions pattern: ``` ^\\w+(:\\w+)*$ ``` 
    requestBody := map[string]interface{}{"key": interface{}(123)} // map[string]interface{} | metadata

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.AccountsApi.AddMetadataToAccount(context.Background(), ledger, address).RequestBody(requestBody).Execute()
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

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## CountAccounts

> CountAccounts(ctx, ledger).Address(address).Metadata(metadata).Execute()

Count the accounts from a ledger

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
    address := "users:.+" // string | Filter accounts by address pattern (regular expression placed between ^ and $). (optional)
    metadata := map[string]interface{}{"key": map[string]interface{}(123)} // map[string]interface{} | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.AccountsApi.CountAccounts(context.Background(), ledger).Address(address).Metadata(metadata).Execute()
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

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetAccount

> AccountResponse GetAccount(ctx, ledger, address).Execute()

Get account by its address

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
    address := "users:001" // string | Exact address of the account. It must match the following regular expressions pattern: ``` ^\\w+(:\\w+)*$ ``` 

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.AccountsApi.GetAccount(context.Background(), ledger, address).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `AccountsApi.GetAccount``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetAccount`: AccountResponse
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

[**AccountResponse**](AccountResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListAccounts

> AccountsCursorResponse ListAccounts(ctx, ledger).PageSize(pageSize).PageSize2(pageSize2).After(after).Address(address).Metadata(metadata).Balance(balance).BalanceOperator(balanceOperator).BalanceOperator2(balanceOperator2).Cursor(cursor).PaginationToken(paginationToken).Execute()

List accounts from a ledger



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
    pageSize := int64(100) // int64 | The maximum number of results to return per page.  (optional) (default to 15)
    pageSize2 := int64(100) // int64 | The maximum number of results to return per page. Deprecated, please use `pageSize` instead.  (optional) (default to 15)
    after := "users:003" // string | Pagination cursor, will return accounts after given address, in descending order. (optional)
    address := "users:.+" // string | Filter accounts by address pattern (regular expression placed between ^ and $). (optional)
    metadata := map[string]interface{}{"key": map[string]interface{}(123)} // map[string]interface{} | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
    balance := int64(2400) // int64 | Filter accounts by their balance (default operator is gte) (optional)
    balanceOperator := "gte" // string | Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not.  (optional)
    balanceOperator2 := "gte" // string | Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not. Deprecated, please use `balanceOperator` instead.  (optional)
    cursor := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
    paginationToken := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use `cursor` instead.  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.AccountsApi.ListAccounts(context.Background(), ledger).PageSize(pageSize).PageSize2(pageSize2).After(after).Address(address).Metadata(metadata).Balance(balance).BalanceOperator(balanceOperator).BalanceOperator2(balanceOperator2).Cursor(cursor).PaginationToken(paginationToken).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `AccountsApi.ListAccounts``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListAccounts`: AccountsCursorResponse
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

 **pageSize** | **int64** | The maximum number of results to return per page.  | [default to 15]
 **pageSize2** | **int64** | The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead.  | [default to 15]
 **after** | **string** | Pagination cursor, will return accounts after given address, in descending order. | 
 **address** | **string** | Filter accounts by address pattern (regular expression placed between ^ and $). | 
 **metadata** | [**map[string]interface{}**](map[string]interface{}.md) | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. | 
 **balance** | **int64** | Filter accounts by their balance (default operator is gte) | 
 **balanceOperator** | **string** | Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not.  | 
 **balanceOperator2** | **string** | Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not. Deprecated, please use &#x60;balanceOperator&#x60; instead.  | 
 **cursor** | **string** | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | 
 **paginationToken** | **string** | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead.  | 

### Return type

[**AccountsCursorResponse**](AccountsCursorResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

