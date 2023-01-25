# \LogsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ListLogs**](LogsApi.md#ListLogs) | **Get** /api/ledger/{ledger}/log | List the logs from a ledger



## ListLogs

> LogsCursorResponse ListLogs(ctx, ledger).PageSize(pageSize).PageSize2(pageSize2).After(after).StartTime(startTime).StartTime2(startTime2).EndTime(endTime).EndTime2(endTime2).Cursor(cursor).PaginationToken(paginationToken).Execute()

List the logs from a ledger



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    "time"
    client "./openapi"
)

func main() {
    ledger := "ledger001" // string | Name of the ledger.
    pageSize := int64(100) // int64 | The maximum number of results to return per page.  (optional) (default to 15)
    pageSize2 := int64(100) // int64 | The maximum number of results to return per page. Deprecated, please use `pageSize` instead.  (optional) (default to 15)
    after := "1234" // string | Pagination cursor, will return the logs after a given ID. (in descending order). (optional)
    startTime := time.Now() // time.Time | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \"2023-01-02T15:04:01Z\" includes the first second of 4th minute).  (optional)
    startTime2 := time.Now() // time.Time | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \"2023-01-02T15:04:01Z\" includes the first second of 4th minute). Deprecated, please use `startTime` instead.  (optional)
    endTime := time.Now() // time.Time | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \"2023-01-02T15:04:01Z\" excludes the first second of 4th minute).  (optional)
    endTime2 := time.Now() // time.Time | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \"2023-01-02T15:04:01Z\" excludes the first second of 4th minute). Deprecated, please use `endTime` instead.  (optional)
    cursor := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
    paginationToken := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use `cursor` instead.  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.LogsApi.ListLogs(context.Background(), ledger).PageSize(pageSize).PageSize2(pageSize2).After(after).StartTime(startTime).StartTime2(startTime2).EndTime(endTime).EndTime2(endTime2).Cursor(cursor).PaginationToken(paginationToken).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `LogsApi.ListLogs``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListLogs`: LogsCursorResponse
    fmt.Fprintf(os.Stdout, "Response from `LogsApi.ListLogs`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 

### Other Parameters

Other parameters are passed through a pointer to a apiListLogsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **pageSize** | **int64** | The maximum number of results to return per page.  | [default to 15]
 **pageSize2** | **int64** | The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead.  | [default to 15]
 **after** | **string** | Pagination cursor, will return the logs after a given ID. (in descending order). | 
 **startTime** | **time.Time** | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute).  | 
 **startTime2** | **time.Time** | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead.  | 
 **endTime** | **time.Time** | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute).  | 
 **endTime2** | **time.Time** | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead.  | 
 **cursor** | **string** | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | 
 **paginationToken** | **string** | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead.  | 

### Return type

[**LogsCursorResponse**](LogsCursorResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

