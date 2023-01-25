# StatsApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**readStats**](StatsApi.md#readStats) | **GET** api/ledger/{ledger}/stats | Get statistics from a ledger |



## readStats

> StatsResponse readStats(ledger)

Get statistics from a ledger

Get statistics from a ledger. (aggregate metrics on accounts and transactions) 

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.StatsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        StatsApi apiInstance = new StatsApi(defaultClient);
        String ledger = "ledger001"; // String | name of the ledger
        try {
            StatsResponse result = apiInstance.readStats(ledger);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling StatsApi#readStats");
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
| **ledger** | **String**| name of the ledger | |

### Return type

[**StatsResponse**](StatsResponse.md)

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

