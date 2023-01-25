# UsersApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**listUsers**](UsersApi.md#listUsers) | **GET** api/auth/users | List users |
| [**readUser**](UsersApi.md#readUser) | **GET** api/auth/users/{userId} | Read user |



## listUsers

> ListUsersResponse listUsers()

List users

List users

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.UsersApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        UsersApi apiInstance = new UsersApi(defaultClient);
        try {
            ListUsersResponse result = apiInstance.listUsers();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling UsersApi#listUsers");
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

[**ListUsersResponse**](ListUsersResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | List of users |  -  |


## readUser

> ReadUserResponse readUser(userId)

Read user

Read user

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.UsersApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        UsersApi apiInstance = new UsersApi(defaultClient);
        String userId = "userId_example"; // String | User ID
        try {
            ReadUserResponse result = apiInstance.readUser(userId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling UsersApi#readUser");
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
| **userId** | **String**| User ID | |

### Return type

[**ReadUserResponse**](ReadUserResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Retrieved user |  -  |

