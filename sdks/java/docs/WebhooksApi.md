# WebhooksApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**activateConfig**](WebhooksApi.md#activateConfig) | **PUT** api/webhooks/configs/{id}/activate | Activate one config |
| [**changeConfigSecret**](WebhooksApi.md#changeConfigSecret) | **PUT** api/webhooks/configs/{id}/secret/change | Change the signing secret of a config |
| [**deactivateConfig**](WebhooksApi.md#deactivateConfig) | **PUT** api/webhooks/configs/{id}/deactivate | Deactivate one config |
| [**deleteConfig**](WebhooksApi.md#deleteConfig) | **DELETE** api/webhooks/configs/{id} | Delete one config |
| [**getManyConfigs**](WebhooksApi.md#getManyConfigs) | **GET** api/webhooks/configs | Get many configs |
| [**insertConfig**](WebhooksApi.md#insertConfig) | **POST** api/webhooks/configs | Insert a new config |
| [**testConfig**](WebhooksApi.md#testConfig) | **GET** api/webhooks/configs/{id}/test | Test one config |



## activateConfig

> ConfigResponse activateConfig(id)

Activate one config

Activate a webhooks config by ID, to start receiving webhooks to its endpoint.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WebhooksApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WebhooksApi apiInstance = new WebhooksApi(defaultClient);
        String id = "4997257d-dfb6-445b-929c-cbe2ab182818"; // String | Config ID
        try {
            ConfigResponse result = apiInstance.activateConfig(id);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WebhooksApi#activateConfig");
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
| **id** | **String**| Config ID | |

### Return type

[**ConfigResponse**](ConfigResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Config successfully activated. |  -  |
| **304** | Config not modified, was already activated. |  -  |


## changeConfigSecret

> ConfigResponse changeConfigSecret(id, configChangeSecret)

Change the signing secret of a config

Change the signing secret of the endpoint of a webhooks config.  If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding) 

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WebhooksApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WebhooksApi apiInstance = new WebhooksApi(defaultClient);
        String id = "4997257d-dfb6-445b-929c-cbe2ab182818"; // String | Config ID
        ConfigChangeSecret configChangeSecret = new ConfigChangeSecret(); // ConfigChangeSecret | 
        try {
            ConfigResponse result = apiInstance.changeConfigSecret(id, configChangeSecret);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WebhooksApi#changeConfigSecret");
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
| **id** | **String**| Config ID | |
| **configChangeSecret** | [**ConfigChangeSecret**](ConfigChangeSecret.md)|  | [optional] |

### Return type

[**ConfigResponse**](ConfigResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Secret successfully changed. |  -  |


## deactivateConfig

> ConfigResponse deactivateConfig(id)

Deactivate one config

Deactivate a webhooks config by ID, to stop receiving webhooks to its endpoint.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WebhooksApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WebhooksApi apiInstance = new WebhooksApi(defaultClient);
        String id = "4997257d-dfb6-445b-929c-cbe2ab182818"; // String | Config ID
        try {
            ConfigResponse result = apiInstance.deactivateConfig(id);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WebhooksApi#deactivateConfig");
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
| **id** | **String**| Config ID | |

### Return type

[**ConfigResponse**](ConfigResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Config successfully deactivated. |  -  |
| **304** | Config not modified, was already deactivated. |  -  |


## deleteConfig

> deleteConfig(id)

Delete one config

Delete a webhooks config by ID.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WebhooksApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WebhooksApi apiInstance = new WebhooksApi(defaultClient);
        String id = "4997257d-dfb6-445b-929c-cbe2ab182818"; // String | Config ID
        try {
            apiInstance.deleteConfig(id);
        } catch (ApiException e) {
            System.err.println("Exception when calling WebhooksApi#deleteConfig");
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
| **id** | **String**| Config ID | |

### Return type

null (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Config successfully deleted. |  -  |


## getManyConfigs

> ConfigsResponse getManyConfigs(id, endpoint)

Get many configs

Sorted by updated date descending

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WebhooksApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WebhooksApi apiInstance = new WebhooksApi(defaultClient);
        String id = "4997257d-dfb6-445b-929c-cbe2ab182818"; // String | Optional filter by Config ID
        String endpoint = "https://example.com"; // String | Optional filter by endpoint URL
        try {
            ConfigsResponse result = apiInstance.getManyConfigs(id, endpoint);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WebhooksApi#getManyConfigs");
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
| **id** | **String**| Optional filter by Config ID | [optional] |
| **endpoint** | **String**| Optional filter by endpoint URL | [optional] |

### Return type

[**ConfigsResponse**](ConfigsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## insertConfig

> ConfigResponse insertConfig(configUser)

Insert a new config

Insert a new webhooks config.  The endpoint should be a valid https URL and be unique.  The secret is the endpoint&#39;s verification secret. If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding)  All eventTypes are converted to lower-case when inserted. 

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WebhooksApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WebhooksApi apiInstance = new WebhooksApi(defaultClient);
        ConfigUser configUser = new ConfigUser(); // ConfigUser | 
        try {
            ConfigResponse result = apiInstance.insertConfig(configUser);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WebhooksApi#insertConfig");
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
| **configUser** | [**ConfigUser**](ConfigUser.md)|  | |

### Return type

[**ConfigResponse**](ConfigResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json, text/plain


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Config created successfully. |  -  |
| **400** | Bad Request |  -  |


## testConfig

> AttemptResponse testConfig(id)

Test one config

Test a config by sending a webhook to its endpoint.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WebhooksApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WebhooksApi apiInstance = new WebhooksApi(defaultClient);
        String id = "4997257d-dfb6-445b-929c-cbe2ab182818"; // String | Config ID
        try {
            AttemptResponse result = apiInstance.testConfig(id);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WebhooksApi#testConfig");
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
| **id** | **String**| Config ID | |

### Return type

[**AttemptResponse**](AttemptResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |

