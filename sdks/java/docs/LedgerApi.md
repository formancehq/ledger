# LedgerApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**getLedgerInfo**](LedgerApi.md#getLedgerInfo) | **GET** api/ledger/{ledger}/_info | Get information about a ledger |



## getLedgerInfo

> LedgerInfoResponse getLedgerInfo(ledger)

Get information about a ledger

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.LedgerApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        LedgerApi apiInstance = new LedgerApi(defaultClient);
        String ledger = "ledger001"; // String | Name of the ledger.
        try {
            LedgerInfoResponse result = apiInstance.getLedgerInfo(ledger);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling LedgerApi#getLedgerInfo");
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

### Return type

[**LedgerInfoResponse**](LedgerInfoResponse.md)

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

