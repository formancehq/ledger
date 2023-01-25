# ScriptApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**runScript**](ScriptApi.md#runScript) | **POST** api/ledger/{ledger}/script | Execute a Numscript |



## runScript

> ScriptResponse runScript(ledger, script, preview)

Execute a Numscript

This route is deprecated, and has been merged into &#x60;POST /{ledger}/transactions&#x60;. 

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ScriptApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ScriptApi apiInstance = new ScriptApi(defaultClient);
        String ledger = "ledger001"; // String | Name of the ledger.
        Script script = new Script(); // Script | 
        Boolean preview = true; // Boolean | Set the preview mode. Preview mode doesn't add the logs to the database or publish a message to the message broker.
        try {
            ScriptResponse result = apiInstance.runScript(ledger, script, preview);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling ScriptApi#runScript");
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
| **script** | [**Script**](Script.md)|  | |
| **preview** | **Boolean**| Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker. | [optional] |

### Return type

[**ScriptResponse**](ScriptResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | On success, it will return a 200 status code, and the resulting transaction under the &#x60;transaction&#x60; field.  On failure, it will also return a 200 status code, and the following fields:   - &#x60;details&#x60;: contains a URL. When there is an error parsing Numscript, the result can be difficult to read—the provided URL will render the error in an easy-to-read format.   - &#x60;errorCode&#x60; and &#x60;error_code&#x60; (deprecated): contains the string code of the error   - &#x60;errorMessage&#x60; and &#x60;error_message&#x60; (deprecated): contains a human-readable indication of what went wrong, for example that an account had insufficient funds, or that there was an error in the provided Numscript.  |  -  |

