# \LedgerApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GetLedgerInfo**](LedgerApi.md#GetLedgerInfo) | **Get** /api/ledger/{ledger}/_info | Get information about a ledger



## GetLedgerInfo

> LedgerInfoResponse GetLedgerInfo(ctx, ledger).Execute()

Get information about a ledger

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

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.LedgerApi.GetLedgerInfo(context.Background(), ledger).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `LedgerApi.GetLedgerInfo``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetLedgerInfo`: LedgerInfoResponse
    fmt.Fprintf(os.Stdout, "Response from `LedgerApi.GetLedgerInfo`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetLedgerInfoRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**LedgerInfoResponse**](LedgerInfoResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

