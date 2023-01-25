# \StatsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ReadStats**](StatsApi.md#ReadStats) | **Get** /api/ledger/{ledger}/stats | Get statistics from a ledger



## ReadStats

> StatsResponse ReadStats(ctx, ledger).Execute()

Get statistics from a ledger



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
    ledger := "ledger001" // string | name of the ledger

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.StatsApi.ReadStats(context.Background(), ledger).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `StatsApi.ReadStats``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ReadStats`: StatsResponse
    fmt.Fprintf(os.Stdout, "Response from `StatsApi.ReadStats`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | name of the ledger | 

### Other Parameters

Other parameters are passed through a pointer to a apiReadStatsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**StatsResponse**](StatsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

