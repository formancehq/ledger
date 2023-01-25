# ClientsApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**addScopeToClient**](ClientsApi.md#addScopeToClient) | **PUT** api/auth/clients/{clientId}/scopes/{scopeId} | Add scope to client |
| [**createClient**](ClientsApi.md#createClient) | **POST** api/auth/clients | Create client |
| [**createSecret**](ClientsApi.md#createSecret) | **POST** api/auth/clients/{clientId}/secrets | Add a secret to a client |
| [**deleteClient**](ClientsApi.md#deleteClient) | **DELETE** api/auth/clients/{clientId} | Delete client |
| [**deleteScopeFromClient**](ClientsApi.md#deleteScopeFromClient) | **DELETE** api/auth/clients/{clientId}/scopes/{scopeId} | Delete scope from client |
| [**deleteSecret**](ClientsApi.md#deleteSecret) | **DELETE** api/auth/clients/{clientId}/secrets/{secretId} | Delete a secret from a client |
| [**listClients**](ClientsApi.md#listClients) | **GET** api/auth/clients | List clients |
| [**readClient**](ClientsApi.md#readClient) | **GET** api/auth/clients/{clientId} | Read client |
| [**updateClient**](ClientsApi.md#updateClient) | **PUT** api/auth/clients/{clientId} | Update client |



## addScopeToClient

> addScopeToClient(clientId, scopeId)

Add scope to client

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ClientsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ClientsApi apiInstance = new ClientsApi(defaultClient);
        String clientId = "clientId_example"; // String | Client ID
        String scopeId = "scopeId_example"; // String | Scope ID
        try {
            apiInstance.addScopeToClient(clientId, scopeId);
        } catch (ApiException e) {
            System.err.println("Exception when calling ClientsApi#addScopeToClient");
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
| **clientId** | **String**| Client ID | |
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
| **204** | Scope added to client |  -  |


## createClient

> CreateClientResponse createClient(body)

Create client

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ClientsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ClientsApi apiInstance = new ClientsApi(defaultClient);
        ClientOptions body = new ClientOptions(); // ClientOptions | 
        try {
            CreateClientResponse result = apiInstance.createClient(body);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling ClientsApi#createClient");
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
| **body** | **ClientOptions**|  | [optional] |

### Return type

[**CreateClientResponse**](CreateClientResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | Client created |  -  |


## createSecret

> CreateSecretResponse createSecret(clientId, body)

Add a secret to a client

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ClientsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ClientsApi apiInstance = new ClientsApi(defaultClient);
        String clientId = "clientId_example"; // String | Client ID
        SecretOptions body = new SecretOptions(); // SecretOptions | 
        try {
            CreateSecretResponse result = apiInstance.createSecret(clientId, body);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling ClientsApi#createSecret");
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
| **clientId** | **String**| Client ID | |
| **body** | **SecretOptions**|  | [optional] |

### Return type

[**CreateSecretResponse**](CreateSecretResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Created secret |  -  |


## deleteClient

> deleteClient(clientId)

Delete client

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ClientsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ClientsApi apiInstance = new ClientsApi(defaultClient);
        String clientId = "clientId_example"; // String | Client ID
        try {
            apiInstance.deleteClient(clientId);
        } catch (ApiException e) {
            System.err.println("Exception when calling ClientsApi#deleteClient");
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
| **clientId** | **String**| Client ID | |

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
| **204** | Client deleted |  -  |


## deleteScopeFromClient

> deleteScopeFromClient(clientId, scopeId)

Delete scope from client

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ClientsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ClientsApi apiInstance = new ClientsApi(defaultClient);
        String clientId = "clientId_example"; // String | Client ID
        String scopeId = "scopeId_example"; // String | Scope ID
        try {
            apiInstance.deleteScopeFromClient(clientId, scopeId);
        } catch (ApiException e) {
            System.err.println("Exception when calling ClientsApi#deleteScopeFromClient");
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
| **clientId** | **String**| Client ID | |
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
| **204** | Scope deleted from client |  -  |


## deleteSecret

> deleteSecret(clientId, secretId)

Delete a secret from a client

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ClientsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ClientsApi apiInstance = new ClientsApi(defaultClient);
        String clientId = "clientId_example"; // String | Client ID
        String secretId = "secretId_example"; // String | Secret ID
        try {
            apiInstance.deleteSecret(clientId, secretId);
        } catch (ApiException e) {
            System.err.println("Exception when calling ClientsApi#deleteSecret");
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
| **clientId** | **String**| Client ID | |
| **secretId** | **String**| Secret ID | |

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
| **204** | Secret deleted |  -  |


## listClients

> ListClientsResponse listClients()

List clients

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ClientsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ClientsApi apiInstance = new ClientsApi(defaultClient);
        try {
            ListClientsResponse result = apiInstance.listClients();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling ClientsApi#listClients");
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

[**ListClientsResponse**](ListClientsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | List of clients |  -  |


## readClient

> ReadClientResponse readClient(clientId)

Read client

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ClientsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ClientsApi apiInstance = new ClientsApi(defaultClient);
        String clientId = "clientId_example"; // String | Client ID
        try {
            ReadClientResponse result = apiInstance.readClient(clientId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling ClientsApi#readClient");
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
| **clientId** | **String**| Client ID | |

### Return type

[**ReadClientResponse**](ReadClientResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Retrieved client |  -  |


## updateClient

> CreateClientResponse updateClient(clientId, body)

Update client

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.ClientsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        ClientsApi apiInstance = new ClientsApi(defaultClient);
        String clientId = "clientId_example"; // String | Client ID
        ClientOptions body = new ClientOptions(); // ClientOptions | 
        try {
            CreateClientResponse result = apiInstance.updateClient(clientId, body);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling ClientsApi#updateClient");
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
| **clientId** | **String**| Client ID | |
| **body** | **ClientOptions**|  | [optional] |

### Return type

[**CreateClientResponse**](CreateClientResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Updated client |  -  |

