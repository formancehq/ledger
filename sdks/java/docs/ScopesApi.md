# ScopesApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addTransientScope**](ScopesApi.md#addTransientScope) | **PUT** api/auth/scopes/{scopeId}/transient/{transientScopeId} | Add a transient scope to a scope |
| [**createScope**](ScopesApi.md#createScope) | **POST** api/auth/scopes | Create scope |
| [**deleteScope**](ScopesApi.md#deleteScope) | **DELETE** api/auth/scopes/{scopeId} | Delete scope |
| [**deleteTransientScope**](ScopesApi.md#deleteTransientScope) | **DELETE** api/auth/scopes/{scopeId}/transient/{transientScopeId} | Delete a transient scope from a scope |
| [**listScopes**](ScopesApi.md#listScopes) | **GET** api/auth/scopes | List scopes |
| [**readScope**](ScopesApi.md#readScope) | **GET** api/auth/scopes/{scopeId} | Read scope |
| [**updateScope**](ScopesApi.md#updateScope) | **PUT** api/auth/scopes/{scopeId} | Update scope |



## addTransientScope

> addTransientScope(scopeId, transientScopeId)

Add a transient scope to a scope

Add a transient scope to a scope

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ScopesApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ScopesApi apiInstance = new ScopesApi(defaultClient);
        String scopeId = "scopeId_example"; // String | Scope ID
        String transientScopeId = "transientScopeId_example"; // String | Transient scope ID
        try {
            apiInstance.addTransientScope(scopeId, transientScopeId);
        } catch (ApiException e) {
            System.err.println("Exception when calling ScopesApi#addTransientScope");
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
| **scopeId** | **String**| Scope ID | |
| **transientScopeId** | **String**| Transient scope ID | |

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
| **204** | Scope added |  -  |


## createScope

> CreateScopeResponse createScope(body)

Create scope

Create scope

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ScopesApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ScopesApi apiInstance = new ScopesApi(defaultClient);
        ScopeOptions body = new ScopeOptions(); // ScopeOptions | 
        try {
            CreateScopeResponse result = apiInstance.createScope(body);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling ScopesApi#createScope");
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
| **body** | **ScopeOptions**|  | [optional] |

### Return type

[**CreateScopeResponse**](CreateScopeResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | Created scope |  -  |


## deleteScope

> deleteScope(scopeId)

Delete scope

Delete scope

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ScopesApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ScopesApi apiInstance = new ScopesApi(defaultClient);
        String scopeId = "scopeId_example"; // String | Scope ID
        try {
            apiInstance.deleteScope(scopeId);
        } catch (ApiException e) {
            System.err.println("Exception when calling ScopesApi#deleteScope");
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
| **scopeId** | **String**| Scope ID | |

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
| **204** | Scope deleted |  -  |


## deleteTransientScope

> deleteTransientScope(scopeId, transientScopeId)

Delete a transient scope from a scope

Delete a transient scope from a scope

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ScopesApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ScopesApi apiInstance = new ScopesApi(defaultClient);
        String scopeId = "scopeId_example"; // String | Scope ID
        String transientScopeId = "transientScopeId_example"; // String | Transient scope ID
        try {
            apiInstance.deleteTransientScope(scopeId, transientScopeId);
        } catch (ApiException e) {
            System.err.println("Exception when calling ScopesApi#deleteTransientScope");
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
| **scopeId** | **String**| Scope ID | |
| **transientScopeId** | **String**| Transient scope ID | |

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
| **204** | Transient scope deleted |  -  |


## listScopes

> ListScopesResponse listScopes()

List scopes

List Scopes

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ScopesApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ScopesApi apiInstance = new ScopesApi(defaultClient);
        try {
            ListScopesResponse result = apiInstance.listScopes();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling ScopesApi#listScopes");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**ListScopesResponse**](ListScopesResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | List of scopes |  -  |


## readScope

> CreateScopeResponse readScope(scopeId)

Read scope

Read scope

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ScopesApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ScopesApi apiInstance = new ScopesApi(defaultClient);
        String scopeId = "scopeId_example"; // String | Scope ID
        try {
            CreateScopeResponse result = apiInstance.readScope(scopeId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling ScopesApi#readScope");
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
| **scopeId** | **String**| Scope ID | |

### Return type

[**CreateScopeResponse**](CreateScopeResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Retrieved scope |  -  |


## updateScope

> CreateScopeResponse updateScope(scopeId, body)

Update scope

Update scope

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ScopesApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ScopesApi apiInstance = new ScopesApi(defaultClient);
        String scopeId = "scopeId_example"; // String | Scope ID
        ScopeOptions body = new ScopeOptions(); // ScopeOptions | 
        try {
            CreateScopeResponse result = apiInstance.updateScope(scopeId, body);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling ScopesApi#updateScope");
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
| **scopeId** | **String**| Scope ID | |
| **body** | **ScopeOptions**|  | [optional] |

### Return type

[**CreateScopeResponse**](CreateScopeResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Updated scope |  -  |

