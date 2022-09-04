# \StatsApi

All URIs are relative to *https://.o.numary.cloud/ledger*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ReadStats**](StatsApi.md#ReadStats) | **Get** /{ledger}/stats | Get Stats



## ReadStats

> StatsResponse ReadStats(ctx, ledger).Execute()

Get Stats



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
    ledger := "ledger001" // string | name of the ledger

    configuration := client.NewConfiguration()
    api_client := client.NewAPIClient(configuration)
    resp, r, err := api_client.StatsApi.ReadStats(context.Background(), ledger).Execute()
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

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

