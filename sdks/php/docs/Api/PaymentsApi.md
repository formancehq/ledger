# Formance\PaymentsApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**connectorsStripeTransfer()**](PaymentsApi.md#connectorsStripeTransfer) | **POST** /api/payments/connectors/stripe/transfer | Transfer funds between Stripe accounts |
| [**getConnectorTask()**](PaymentsApi.md#getConnectorTask) | **GET** /api/payments/connectors/{connector}/tasks/{taskId} | Read a specific task of the connector |
| [**getPayment()**](PaymentsApi.md#getPayment) | **GET** /api/payments/payments/{paymentId} | Get a payment |
| [**installConnector()**](PaymentsApi.md#installConnector) | **POST** /api/payments/connectors/{connector} | Install a connector |
| [**listAllConnectors()**](PaymentsApi.md#listAllConnectors) | **GET** /api/payments/connectors | List all installed connectors |
| [**listConfigsAvailableConnectors()**](PaymentsApi.md#listConfigsAvailableConnectors) | **GET** /api/payments/connectors/configs | List the configs of each available connector |
| [**listConnectorTasks()**](PaymentsApi.md#listConnectorTasks) | **GET** /api/payments/connectors/{connector}/tasks | List tasks from a connector |
| [**listPayments()**](PaymentsApi.md#listPayments) | **GET** /api/payments/payments | List payments |
| [**paymentslistAccounts()**](PaymentsApi.md#paymentslistAccounts) | **GET** /api/payments/accounts | List accounts |
| [**readConnectorConfig()**](PaymentsApi.md#readConnectorConfig) | **GET** /api/payments/connectors/{connector}/config | Read the config of a connector |
| [**resetConnector()**](PaymentsApi.md#resetConnector) | **POST** /api/payments/connectors/{connector}/reset | Reset a connector |
| [**uninstallConnector()**](PaymentsApi.md#uninstallConnector) | **DELETE** /api/payments/connectors/{connector} | Uninstall a connector |


## `connectorsStripeTransfer()`

```php
connectorsStripeTransfer($stripe_transfer_request): object
```

Transfer funds between Stripe accounts

Execute a transfer between two Stripe accounts.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$stripe_transfer_request = new \Formance\Model\StripeTransferRequest(); // \Formance\Model\StripeTransferRequest

try {
    $result = $apiInstance->connectorsStripeTransfer($stripe_transfer_request);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->connectorsStripeTransfer: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **stripe_transfer_request** | [**\Formance\Model\StripeTransferRequest**](../Model/StripeTransferRequest.md)|  | |

### Return type

**object**

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `getConnectorTask()`

```php
getConnectorTask($connector, $task_id): \Formance\Model\TaskResponse
```

Read a specific task of the connector

Get a specific task associated to the connector.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$connector = new \Formance\Model\Connector(); // Connector | The name of the connector.
$task_id = task1; // string | The task ID.

try {
    $result = $apiInstance->getConnectorTask($connector, $task_id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->getConnectorTask: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **connector** | [**Connector**](../Model/.md)| The name of the connector. | |
| **task_id** | **string**| The task ID. | |

### Return type

[**\Formance\Model\TaskResponse**](../Model/TaskResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `getPayment()`

```php
getPayment($payment_id): \Formance\Model\PaymentResponse
```

Get a payment

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$payment_id = XXX; // string | The payment ID.

try {
    $result = $apiInstance->getPayment($payment_id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->getPayment: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **payment_id** | **string**| The payment ID. | |

### Return type

[**\Formance\Model\PaymentResponse**](../Model/PaymentResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `installConnector()`

```php
installConnector($connector, $connector_config)
```

Install a connector

Install a connector by its name and config.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$connector = new \Formance\Model\Connector(); // Connector | The name of the connector.
$connector_config = new \Formance\Model\ConnectorConfig(); // \Formance\Model\ConnectorConfig

try {
    $apiInstance->installConnector($connector, $connector_config);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->installConnector: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **connector** | [**Connector**](../Model/.md)| The name of the connector. | |
| **connector_config** | [**\Formance\Model\ConnectorConfig**](../Model/ConnectorConfig.md)|  | |

### Return type

void (empty response body)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `listAllConnectors()`

```php
listAllConnectors(): \Formance\Model\ConnectorsResponse
```

List all installed connectors

List all installed connectors.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);

try {
    $result = $apiInstance->listAllConnectors();
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->listAllConnectors: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**\Formance\Model\ConnectorsResponse**](../Model/ConnectorsResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `listConfigsAvailableConnectors()`

```php
listConfigsAvailableConnectors(): \Formance\Model\ConnectorsConfigsResponse
```

List the configs of each available connector

List the configs of each available connector.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);

try {
    $result = $apiInstance->listConfigsAvailableConnectors();
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->listConfigsAvailableConnectors: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**\Formance\Model\ConnectorsConfigsResponse**](../Model/ConnectorsConfigsResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `listConnectorTasks()`

```php
listConnectorTasks($connector, $page_size, $cursor): \Formance\Model\TasksCursor
```

List tasks from a connector

List all tasks associated with this connector.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$connector = new \Formance\Model\Connector(); // Connector | The name of the connector.
$page_size = 100; // int | The maximum number of results to return per page.
$cursor = aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==; // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.

try {
    $result = $apiInstance->listConnectorTasks($connector, $page_size, $cursor);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->listConnectorTasks: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **connector** | [**Connector**](../Model/.md)| The name of the connector. | |
| **page_size** | **int**| The maximum number of results to return per page. | [optional] [default to 15] |
| **cursor** | **string**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. | [optional] |

### Return type

[**\Formance\Model\TasksCursor**](../Model/TasksCursor.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `listPayments()`

```php
listPayments($page_size, $cursor, $sort): \Formance\Model\PaymentsCursor
```

List payments

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$page_size = 100; // int | The maximum number of results to return per page.
$cursor = aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==; // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.
$sort = date:asc,status:desc; // string[] | Fields used to sort payments (default is date:desc).

try {
    $result = $apiInstance->listPayments($page_size, $cursor, $sort);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->listPayments: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **page_size** | **int**| The maximum number of results to return per page. | [optional] [default to 15] |
| **cursor** | **string**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. | [optional] |
| **sort** | [**string[]**](../Model/string.md)| Fields used to sort payments (default is date:desc). | [optional] |

### Return type

[**\Formance\Model\PaymentsCursor**](../Model/PaymentsCursor.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `paymentslistAccounts()`

```php
paymentslistAccounts($page_size, $cursor, $sort): \Formance\Model\AccountsCursor
```

List accounts

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$page_size = 100; // int | The maximum number of results to return per page.
$cursor = aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==; // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.
$sort = date:asc,status:desc; // string[] | Fields used to sort payments (default is date:desc).

try {
    $result = $apiInstance->paymentslistAccounts($page_size, $cursor, $sort);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->paymentslistAccounts: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **page_size** | **int**| The maximum number of results to return per page. | [optional] [default to 15] |
| **cursor** | **string**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. | [optional] |
| **sort** | [**string[]**](../Model/string.md)| Fields used to sort payments (default is date:desc). | [optional] |

### Return type

[**\Formance\Model\AccountsCursor**](../Model/AccountsCursor.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `readConnectorConfig()`

```php
readConnectorConfig($connector): \Formance\Model\ConnectorConfigResponse
```

Read the config of a connector

Read connector config

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$connector = new \Formance\Model\Connector(); // Connector | The name of the connector.

try {
    $result = $apiInstance->readConnectorConfig($connector);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->readConnectorConfig: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **connector** | [**Connector**](../Model/.md)| The name of the connector. | |

### Return type

[**\Formance\Model\ConnectorConfigResponse**](../Model/ConnectorConfigResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `resetConnector()`

```php
resetConnector($connector)
```

Reset a connector

Reset a connector by its name. It will remove the connector and ALL PAYMENTS generated with it.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$connector = new \Formance\Model\Connector(); // Connector | The name of the connector.

try {
    $apiInstance->resetConnector($connector);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->resetConnector: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **connector** | [**Connector**](../Model/.md)| The name of the connector. | |

### Return type

void (empty response body)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `uninstallConnector()`

```php
uninstallConnector($connector)
```

Uninstall a connector

Uninstall a connector by its name.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\PaymentsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$connector = new \Formance\Model\Connector(); // Connector | The name of the connector.

try {
    $apiInstance->uninstallConnector($connector);
} catch (Exception $e) {
    echo 'Exception when calling PaymentsApi->uninstallConnector: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **connector** | [**Connector**](../Model/.md)| The name of the connector. | |

### Return type

void (empty response body)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
