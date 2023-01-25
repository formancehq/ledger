# MappingApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**getMapping**](MappingApi.md#getMapping) | **GET** api/ledger/{ledger}/mapping | Get the mapping of a ledger |
| [**updateMapping**](MappingApi.md#updateMapping) | **PUT** api/ledger/{ledger}/mapping | Update the mapping of a ledger |



## getMapping

> MappingResponse getMapping(ledger)

Get the mapping of a ledger

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.MappingApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        MappingApi apiInstance = new MappingApi(defaultClient);
        String ledger = "ledger001"; // String | Name of the ledger.
        try {
            MappingResponse result = apiInstance.getMapping(ledger);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling MappingApi#getMapping");
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

[**MappingResponse**](MappingResponse.md)

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


## updateMapping

> MappingResponse updateMapping(ledger, mapping)

Update the mapping of a ledger

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.MappingApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        MappingApi apiInstance = new MappingApi(defaultClient);
        String ledger = "ledger001"; // String | Name of the ledger.
        Mapping mapping = new Mapping(); // Mapping | 
        try {
            MappingResponse result = apiInstance.updateMapping(ledger, mapping);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling MappingApi#updateMapping");
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
| **mapping** | [**Mapping**](Mapping.md)|  | |

### Return type

[**MappingResponse**](MappingResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |
| **0** | Error |  -  |

