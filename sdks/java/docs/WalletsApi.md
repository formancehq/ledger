# WalletsApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**confirmHold**](WalletsApi.md#confirmHold) | **POST** api/wallets/holds/{hold_id}/confirm | Confirm a hold |
| [**createBalance**](WalletsApi.md#createBalance) | **POST** api/wallets/wallets/{id}/balances | Create a balance |
| [**createWallet**](WalletsApi.md#createWallet) | **POST** api/wallets/wallets | Create a new wallet |
| [**creditWallet**](WalletsApi.md#creditWallet) | **POST** api/wallets/wallets/{id}/credit | Credit a wallet |
| [**debitWallet**](WalletsApi.md#debitWallet) | **POST** api/wallets/wallets/{id}/debit | Debit a wallet |
| [**getBalance**](WalletsApi.md#getBalance) | **GET** api/wallets/wallets/{id}/balances/{balanceName} | Get detailed balance |
| [**getHold**](WalletsApi.md#getHold) | **GET** api/wallets/holds/{holdID} | Get a hold |
| [**getHolds**](WalletsApi.md#getHolds) | **GET** api/wallets/holds | Get all holds for a wallet |
| [**getTransactions**](WalletsApi.md#getTransactions) | **GET** api/wallets/transactions |  |
| [**getWallet**](WalletsApi.md#getWallet) | **GET** api/wallets/wallets/{id} | Get a wallet |
| [**listBalances**](WalletsApi.md#listBalances) | **GET** api/wallets/wallets/{id}/balances | List balances of a wallet |
| [**listWallets**](WalletsApi.md#listWallets) | **GET** api/wallets/wallets | List all wallets |
| [**updateWallet**](WalletsApi.md#updateWallet) | **PATCH** api/wallets/wallets/{id} | Update a wallet |
| [**voidHold**](WalletsApi.md#voidHold) | **POST** api/wallets/holds/{hold_id}/void | Cancel a hold |
| [**walletsgetServerInfo**](WalletsApi.md#walletsgetServerInfo) | **GET** api/wallets/_info | Get server info |



## confirmHold

> confirmHold(holdId, confirmHoldRequest)

Confirm a hold

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String holdId = "holdId_example"; // String | 
        ConfirmHoldRequest confirmHoldRequest = new ConfirmHoldRequest(); // ConfirmHoldRequest | 
        try {
            apiInstance.confirmHold(holdId, confirmHoldRequest);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#confirmHold");
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
| **holdId** | **String**|  | |
| **confirmHoldRequest** | [**ConfirmHoldRequest**](ConfirmHoldRequest.md)|  | [optional] |

### Return type

null (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | Hold successfully confirmed, funds moved back to initial destination |  -  |
| **0** | Error |  -  |


## createBalance

> CreateBalanceResponse createBalance(id, body)

Create a balance

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String id = "id_example"; // String | 
        Balance body = new Balance(); // Balance | 
        try {
            CreateBalanceResponse result = apiInstance.createBalance(id, body);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#createBalance");
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
| **id** | **String**|  | |
| **body** | **Balance**|  | [optional] |

### Return type

[**CreateBalanceResponse**](CreateBalanceResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | Created balance |  -  |
| **0** | Error |  -  |


## createWallet

> CreateWalletResponse createWallet(createWalletRequest)

Create a new wallet

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        CreateWalletRequest createWalletRequest = new CreateWalletRequest(); // CreateWalletRequest | 
        try {
            CreateWalletResponse result = apiInstance.createWallet(createWalletRequest);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#createWallet");
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
| **createWalletRequest** | [**CreateWalletRequest**](CreateWalletRequest.md)|  | [optional] |

### Return type

[**CreateWalletResponse**](CreateWalletResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | Wallet created |  -  |
| **0** | Error |  -  |


## creditWallet

> creditWallet(id, creditWalletRequest)

Credit a wallet

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String id = "id_example"; // String | 
        CreditWalletRequest creditWalletRequest = new CreditWalletRequest(); // CreditWalletRequest | 
        try {
            apiInstance.creditWallet(id, creditWalletRequest);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#creditWallet");
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
| **id** | **String**|  | |
| **creditWalletRequest** | [**CreditWalletRequest**](CreditWalletRequest.md)|  | [optional] |

### Return type

null (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | Wallet successfully credited |  -  |
| **0** | Error |  -  |


## debitWallet

> DebitWalletResponse debitWallet(id, debitWalletRequest)

Debit a wallet

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String id = "id_example"; // String | 
        DebitWalletRequest debitWalletRequest = new DebitWalletRequest(); // DebitWalletRequest | 
        try {
            DebitWalletResponse result = apiInstance.debitWallet(id, debitWalletRequest);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#debitWallet");
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
| **id** | **String**|  | |
| **debitWalletRequest** | [**DebitWalletRequest**](DebitWalletRequest.md)|  | [optional] |

### Return type

[**DebitWalletResponse**](DebitWalletResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Wallet successfully debited as a pending hold |  -  |
| **204** | Wallet successfully debited |  -  |
| **0** | Error |  -  |


## getBalance

> GetBalanceResponse getBalance(id, balanceName)

Get detailed balance

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String id = "id_example"; // String | 
        String balanceName = "balanceName_example"; // String | 
        try {
            GetBalanceResponse result = apiInstance.getBalance(id, balanceName);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#getBalance");
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
| **id** | **String**|  | |
| **balanceName** | **String**|  | |

### Return type

[**GetBalanceResponse**](GetBalanceResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Balance summary |  -  |
| **0** | Error |  -  |


## getHold

> GetHoldResponse getHold(holdID)

Get a hold

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String holdID = "holdID_example"; // String | The hold ID
        try {
            GetHoldResponse result = apiInstance.getHold(holdID);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#getHold");
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
| **holdID** | **String**| The hold ID | |

### Return type

[**GetHoldResponse**](GetHoldResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Holds |  -  |
| **0** | Error |  -  |


## getHolds

> GetHoldsResponse getHolds(pageSize, walletID, metadata, cursor)

Get all holds for a wallet

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        Integer pageSize = 15; // Integer | The maximum number of results to return per page
        String walletID = "wallet1"; // String | The wallet to filter on
        Object metadata = new HashMap(); // Object | Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below.
        String cursor = "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="; // String | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set. 
        try {
            GetHoldsResponse result = apiInstance.getHolds(pageSize, walletID, metadata, cursor);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#getHolds");
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
| **pageSize** | **Integer**| The maximum number of results to return per page | [optional] [default to 15] |
| **walletID** | **String**| The wallet to filter on | [optional] |
| **metadata** | [**Object**](.md)| Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below. | [optional] |
| **cursor** | **String**| Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  | [optional] |

### Return type

[**GetHoldsResponse**](GetHoldsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Holds |  -  |
| **0** | Error |  -  |


## getTransactions

> GetTransactionsResponse getTransactions(pageSize, walletId, cursor)



### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        Integer pageSize = 15; // Integer | The maximum number of results to return per page
        String walletId = "wallet1"; // String | A wallet ID to filter on
        String cursor = "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="; // String | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set. 
        try {
            GetTransactionsResponse result = apiInstance.getTransactions(pageSize, walletId, cursor);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#getTransactions");
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
| **pageSize** | **Integer**| The maximum number of results to return per page | [optional] [default to 15] |
| **walletId** | **String**| A wallet ID to filter on | [optional] |
| **cursor** | **String**| Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set.  | [optional] |

### Return type

[**GetTransactionsResponse**](GetTransactionsResponse.md)

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


## getWallet

> GetWalletResponse getWallet(id)

Get a wallet

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String id = "id_example"; // String | 
        try {
            GetWalletResponse result = apiInstance.getWallet(id);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#getWallet");
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
| **id** | **String**|  | |

### Return type

[**GetWalletResponse**](GetWalletResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Wallet |  -  |
| **404** | Wallet not found |  -  |
| **0** | Error |  -  |


## listBalances

> ListBalancesResponse listBalances(id)

List balances of a wallet

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String id = "id_example"; // String | 
        try {
            ListBalancesResponse result = apiInstance.listBalances(id);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#listBalances");
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
| **id** | **String**|  | |

### Return type

[**ListBalancesResponse**](ListBalancesResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Balances list |  -  |


## listWallets

> ListWalletsResponse listWallets(name, metadata, pageSize, cursor)

List all wallets

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String name = "wallet1"; // String | Filter on wallet name
        Object metadata = new HashMap(); // Object | Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below.
        Integer pageSize = 15; // Integer | The maximum number of results to return per page
        String cursor = "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="; // String | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set. 
        try {
            ListWalletsResponse result = apiInstance.listWallets(name, metadata, pageSize, cursor);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#listWallets");
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
| **name** | **String**| Filter on wallet name | [optional] |
| **metadata** | [**Object**](.md)| Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below. | [optional] |
| **pageSize** | **Integer**| The maximum number of results to return per page | [optional] [default to 15] |
| **cursor** | **String**| Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  | [optional] |

### Return type

[**ListWalletsResponse**](ListWalletsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## updateWallet

> updateWallet(id, updateWalletRequest)

Update a wallet

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String id = "id_example"; // String | 
        UpdateWalletRequest updateWalletRequest = new UpdateWalletRequest(); // UpdateWalletRequest | 
        try {
            apiInstance.updateWallet(id, updateWalletRequest);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#updateWallet");
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
| **id** | **String**|  | |
| **updateWalletRequest** | [**UpdateWalletRequest**](UpdateWalletRequest.md)|  | [optional] |

### Return type

null (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | Wallet successfully updated |  -  |
| **0** | Error |  -  |


## voidHold

> voidHold(holdId)

Cancel a hold

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        String holdId = "holdId_example"; // String | 
        try {
            apiInstance.voidHold(holdId);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#voidHold");
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
| **holdId** | **String**|  | |

### Return type

null (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | Hold successfully cancelled, funds returned to wallet |  -  |
| **0** | Error |  -  |


## walletsgetServerInfo

> ServerInfo walletsgetServerInfo()

Get server info

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.WalletsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        WalletsApi apiInstance = new WalletsApi(defaultClient);
        try {
            ServerInfo result = apiInstance.walletsgetServerInfo();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling WalletsApi#walletsgetServerInfo");
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

[**ServerInfo**](ServerInfo.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Server information |  -  |
| **0** | Error |  -  |

