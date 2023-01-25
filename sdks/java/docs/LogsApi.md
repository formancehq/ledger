# LogsApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**listLogs**](LogsApi.md#listLogs) | **GET** api/ledger/{ledger}/log | List the logs from a ledger |



## listLogs

> LogsCursorResponse listLogs(ledger, pageSize, pageSize2, after, startTime, startTime2, endTime, endTime2, cursor, paginationToken)

List the logs from a ledger

List the logs from a ledger, sorted by ID in descending order.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.LogsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        LogsApi apiInstance = new LogsApi(defaultClient);
        String ledger = "ledger001"; // String | Name of the ledger.
        Long pageSize = 15L; // Long | The maximum number of results to return per page. 
        Long pageSize2 = 15L; // Long | The maximum number of results to return per page. Deprecated, please use `pageSize` instead. 
        String after = "1234"; // String | Pagination cursor, will return the logs after a given ID. (in descending order).
        OffsetDateTime startTime = OffsetDateTime.now(); // OffsetDateTime | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \"2023-01-02T15:04:01Z\" includes the first second of 4th minute). 
        OffsetDateTime startTime2 = OffsetDateTime.now(); // OffsetDateTime | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \"2023-01-02T15:04:01Z\" includes the first second of 4th minute). Deprecated, please use `startTime` instead. 
        OffsetDateTime endTime = OffsetDateTime.now(); // OffsetDateTime | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \"2023-01-02T15:04:01Z\" excludes the first second of 4th minute). 
        OffsetDateTime endTime2 = OffsetDateTime.now(); // OffsetDateTime | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \"2023-01-02T15:04:01Z\" excludes the first second of 4th minute). Deprecated, please use `endTime` instead. 
        String cursor = "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="; // String | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
        String paginationToken = "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="; // String | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use `cursor` instead. 
        try {
            LogsCursorResponse result = apiInstance.listLogs(ledger, pageSize, pageSize2, after, startTime, startTime2, endTime, endTime2, cursor, paginationToken);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling LogsApi#listLogs");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **ledger** | **String**| Name of the ledger. | |
| **pageSize** | **Long**| The maximum number of results to return per page.  | [optional] [default to 15] |
| **pageSize2** | **Long**| The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead.  | [optional] [default to 15] |
| **after** | **String**| Pagination cursor, will return the logs after a given ID. (in descending order). | [optional] |
| **startTime** | **OffsetDateTime**| Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute).  | [optional] |
| **startTime2** | **OffsetDateTime**| Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead.  | [optional] |
| **endTime** | **OffsetDateTime**| Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute).  | [optional] |
| **endTime2** | **OffsetDateTime**| Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead.  | [optional] |
| **cursor** | **String**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | [optional] |
| **paginationToken** | **String**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead.  | [optional] |

### Return type

[**LogsCursorResponse**](LogsCursorResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **0** | Error |  -  |

