# PaymentsApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**connectorsStripeTransfer**](PaymentsApi.md#connectorsStripeTransfer) | **POST** api/payments/connectors/stripe/transfer | Transfer funds between Stripe accounts |
| [**getConnectorTask**](PaymentsApi.md#getConnectorTask) | **GET** api/payments/connectors/{connector}/tasks/{taskId} | Read a specific task of the connector |
| [**getPayment**](PaymentsApi.md#getPayment) | **GET** api/payments/payments/{paymentId} | Get a payment |
| [**installConnector**](PaymentsApi.md#installConnector) | **POST** api/payments/connectors/{connector} | Install a connector |
| [**listAllConnectors**](PaymentsApi.md#listAllConnectors) | **GET** api/payments/connectors | List all installed connectors |
| [**listConfigsAvailableConnectors**](PaymentsApi.md#listConfigsAvailableConnectors) | **GET** api/payments/connectors/configs | List the configs of each available connector |
| [**listConnectorTasks**](PaymentsApi.md#listConnectorTasks) | **GET** api/payments/connectors/{connector}/tasks | List tasks from a connector |
| [**listPayments**](PaymentsApi.md#listPayments) | **GET** api/payments/payments | List payments |
| [**paymentslistAccounts**](PaymentsApi.md#paymentslistAccounts) | **GET** api/payments/accounts | List accounts |
| [**readConnectorConfig**](PaymentsApi.md#readConnectorConfig) | **GET** api/payments/connectors/{connector}/config | Read the config of a connector |
| [**resetConnector**](PaymentsApi.md#resetConnector) | **POST** api/payments/connectors/{connector}/reset | Reset a connector |
| [**uninstallConnector**](PaymentsApi.md#uninstallConnector) | **DELETE** api/payments/connectors/{connector} | Uninstall a connector |



## connectorsStripeTransfer

> Object connectorsStripeTransfer(stripeTransferRequest)

Transfer funds between Stripe accounts

Execute a transfer between two Stripe accounts.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        StripeTransferRequest stripeTransferRequest = new StripeTransferRequest(); // StripeTransferRequest | 
        try {
            Object result = apiInstance.connectorsStripeTransfer(stripeTransferRequest);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#connectorsStripeTransfer");
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
| **stripeTransferRequest** | [**StripeTransferRequest**](StripeTransferRequest.md)|  | |

### Return type

**Object**

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## getConnectorTask

> TaskResponse getConnectorTask(connector, taskId)

Read a specific task of the connector

Get a specific task associated to the connector.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        Connector connector = Connector.fromValue("STRIPE"); // Connector | The name of the connector.
        String taskId = "task1"; // String | The task ID.
        try {
            TaskResponse result = apiInstance.getConnectorTask(connector, taskId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#getConnectorTask");
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
| **connector** | [**Connector**](.md)| The name of the connector. | [enum: STRIPE, DUMMY-PAY, WISE, MODULR, CURRENCY-CLOUD, BANKING-CIRCLE] |
| **taskId** | **String**| The task ID. | |

### Return type

[**TaskResponse**](TaskResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## getPayment

> PaymentResponse getPayment(paymentId)

Get a payment

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        String paymentId = "XXX"; // String | The payment ID.
        try {
            PaymentResponse result = apiInstance.getPayment(paymentId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#getPayment");
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
| **paymentId** | **String**| The payment ID. | |

### Return type

[**PaymentResponse**](PaymentResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## installConnector

> installConnector(connector, connectorConfig)

Install a connector

Install a connector by its name and config.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        Connector connector = Connector.fromValue("STRIPE"); // Connector | The name of the connector.
        ConnectorConfig connectorConfig = new ConnectorConfig(); // ConnectorConfig | 
        try {
            apiInstance.installConnector(connector, connectorConfig);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#installConnector");
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
| **connector** | [**Connector**](.md)| The name of the connector. | [enum: STRIPE, DUMMY-PAY, WISE, MODULR, CURRENCY-CLOUD, BANKING-CIRCLE] |
| **connectorConfig** | [**ConnectorConfig**](ConnectorConfig.md)|  | |

### Return type

null (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **204** | No content |  -  |


## listAllConnectors

> ConnectorsResponse listAllConnectors()

List all installed connectors

List all installed connectors.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        try {
            ConnectorsResponse result = apiInstance.listAllConnectors();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#listAllConnectors");
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

[**ConnectorsResponse**](ConnectorsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## listConfigsAvailableConnectors

> ConnectorsConfigsResponse listConfigsAvailableConnectors()

List the configs of each available connector

List the configs of each available connector.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        try {
            ConnectorsConfigsResponse result = apiInstance.listConfigsAvailableConnectors();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#listConfigsAvailableConnectors");
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

[**ConnectorsConfigsResponse**](ConnectorsConfigsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## listConnectorTasks

> TasksCursor listConnectorTasks(connector, pageSize, cursor)

List tasks from a connector

List all tasks associated with this connector.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        Connector connector = Connector.fromValue("STRIPE"); // Connector | The name of the connector.
        Long pageSize = 15L; // Long | The maximum number of results to return per page. 
        String cursor = "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="; // String | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
        try {
            TasksCursor result = apiInstance.listConnectorTasks(connector, pageSize, cursor);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#listConnectorTasks");
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
| **connector** | [**Connector**](.md)| The name of the connector. | [enum: STRIPE, DUMMY-PAY, WISE, MODULR, CURRENCY-CLOUD, BANKING-CIRCLE] |
| **pageSize** | **Long**| The maximum number of results to return per page.  | [optional] [default to 15] |
| **cursor** | **String**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | [optional] |

### Return type

[**TasksCursor**](TasksCursor.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## listPayments

> PaymentsCursor listPayments(pageSize, cursor, sort)

List payments

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        Long pageSize = 15L; // Long | The maximum number of results to return per page. 
        String cursor = "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="; // String | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
        List<String> sort = Arrays.asList(); // List<String> | Fields used to sort payments (default is date:desc).
        try {
            PaymentsCursor result = apiInstance.listPayments(pageSize, cursor, sort);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#listPayments");
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
| **pageSize** | **Long**| The maximum number of results to return per page.  | [optional] [default to 15] |
| **cursor** | **String**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | [optional] |
| **sort** | [**List&lt;String&gt;**](String.md)| Fields used to sort payments (default is date:desc). | [optional] |

### Return type

[**PaymentsCursor**](PaymentsCursor.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## paymentslistAccounts

> AccountsCursor paymentslistAccounts(pageSize, cursor, sort)

List accounts

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        Long pageSize = 15L; // Long | The maximum number of results to return per page. 
        String cursor = "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ=="; // String | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. 
        List<String> sort = Arrays.asList(); // List<String> | Fields used to sort payments (default is date:desc).
        try {
            AccountsCursor result = apiInstance.paymentslistAccounts(pageSize, cursor, sort);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#paymentslistAccounts");
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
| **pageSize** | **Long**| The maximum number of results to return per page.  | [optional] [default to 15] |
| **cursor** | **String**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | [optional] |
| **sort** | [**List&lt;String&gt;**](String.md)| Fields used to sort payments (default is date:desc). | [optional] |

### Return type

[**AccountsCursor**](AccountsCursor.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## readConnectorConfig

> ConnectorConfigResponse readConnectorConfig(connector)

Read the config of a connector

Read connector config

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        Connector connector = Connector.fromValue("STRIPE"); // Connector | The name of the connector.
        try {
            ConnectorConfigResponse result = apiInstance.readConnectorConfig(connector);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#readConnectorConfig");
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
| **connector** | [**Connector**](.md)| The name of the connector. | [enum: STRIPE, DUMMY-PAY, WISE, MODULR, CURRENCY-CLOUD, BANKING-CIRCLE] |

### Return type

[**ConnectorConfigResponse**](ConnectorConfigResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | OK |  -  |


## resetConnector

> resetConnector(connector)

Reset a connector

Reset a connector by its name. It will remove the connector and ALL PAYMENTS generated with it. 

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        Connector connector = Connector.fromValue("STRIPE"); // Connector | The name of the connector.
        try {
            apiInstance.resetConnector(connector);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#resetConnector");
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
| **connector** | [**Connector**](.md)| The name of the connector. | [enum: STRIPE, DUMMY-PAY, WISE, MODULR, CURRENCY-CLOUD, BANKING-CIRCLE] |

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
| **204** | No content |  -  |


## uninstallConnector

> uninstallConnector(connector)

Uninstall a connector

Uninstall a connector by its name.

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.PaymentsApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        PaymentsApi apiInstance = new PaymentsApi(defaultClient);
        Connector connector = Connector.fromValue("STRIPE"); // Connector | The name of the connector.
        try {
            apiInstance.uninstallConnector(connector);
        } catch (ApiException e) {
            System.err.println("Exception when calling PaymentsApi#uninstallConnector");
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
| **connector** | [**Connector**](.md)| The name of the connector. | [enum: STRIPE, DUMMY-PAY, WISE, MODULR, CURRENCY-CLOUD, BANKING-CIRCLE] |

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
| **204** | No content |  -  |

