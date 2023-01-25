# Formance\WalletsApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**confirmHold()**](WalletsApi.md#confirmHold) | **POST** /api/wallets/holds/{hold_id}/confirm | Confirm a hold |
| [**createBalance()**](WalletsApi.md#createBalance) | **POST** /api/wallets/wallets/{id}/balances | Create a balance |
| [**createWallet()**](WalletsApi.md#createWallet) | **POST** /api/wallets/wallets | Create a new wallet |
| [**creditWallet()**](WalletsApi.md#creditWallet) | **POST** /api/wallets/wallets/{id}/credit | Credit a wallet |
| [**debitWallet()**](WalletsApi.md#debitWallet) | **POST** /api/wallets/wallets/{id}/debit | Debit a wallet |
| [**getBalance()**](WalletsApi.md#getBalance) | **GET** /api/wallets/wallets/{id}/balances/{balanceName} | Get detailed balance |
| [**getHold()**](WalletsApi.md#getHold) | **GET** /api/wallets/holds/{holdID} | Get a hold |
| [**getHolds()**](WalletsApi.md#getHolds) | **GET** /api/wallets/holds | Get all holds for a wallet |
| [**getTransactions()**](WalletsApi.md#getTransactions) | **GET** /api/wallets/transactions |  |
| [**getWallet()**](WalletsApi.md#getWallet) | **GET** /api/wallets/wallets/{id} | Get a wallet |
| [**listBalances()**](WalletsApi.md#listBalances) | **GET** /api/wallets/wallets/{id}/balances | List balances of a wallet |
| [**listWallets()**](WalletsApi.md#listWallets) | **GET** /api/wallets/wallets | List all wallets |
| [**updateWallet()**](WalletsApi.md#updateWallet) | **PATCH** /api/wallets/wallets/{id} | Update a wallet |
| [**voidHold()**](WalletsApi.md#voidHold) | **POST** /api/wallets/holds/{hold_id}/void | Cancel a hold |
| [**walletsgetServerInfo()**](WalletsApi.md#walletsgetServerInfo) | **GET** /api/wallets/_info | Get server info |


## `confirmHold()`

```php
confirmHold($hold_id, $confirm_hold_request)
```

Confirm a hold

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$hold_id = 'hold_id_example'; // string
$confirm_hold_request = new \Formance\Model\ConfirmHoldRequest(); // \Formance\Model\ConfirmHoldRequest

try {
    $apiInstance->confirmHold($hold_id, $confirm_hold_request);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->confirmHold: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **hold_id** | **string**|  | |
| **confirm_hold_request** | [**\Formance\Model\ConfirmHoldRequest**](../Model/ConfirmHoldRequest.md)|  | [optional] |

### Return type

void (empty response body)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `createBalance()`

```php
createBalance($id, $body): \Formance\Model\CreateBalanceResponse
```

Create a balance

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 'id_example'; // string
$body = new \Formance\Model\Balance(); // \Formance\Model\Balance

try {
    $result = $apiInstance->createBalance($id, $body);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->createBalance: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**|  | |
| **body** | **\Formance\Model\Balance**|  | [optional] |

### Return type

[**\Formance\Model\CreateBalanceResponse**](../Model/CreateBalanceResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `createWallet()`

```php
createWallet($create_wallet_request): \Formance\Model\CreateWalletResponse
```

Create a new wallet

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$create_wallet_request = new \Formance\Model\CreateWalletRequest(); // \Formance\Model\CreateWalletRequest

try {
    $result = $apiInstance->createWallet($create_wallet_request);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->createWallet: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **create_wallet_request** | [**\Formance\Model\CreateWalletRequest**](../Model/CreateWalletRequest.md)|  | [optional] |

### Return type

[**\Formance\Model\CreateWalletResponse**](../Model/CreateWalletResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `creditWallet()`

```php
creditWallet($id, $credit_wallet_request)
```

Credit a wallet

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 'id_example'; // string
$credit_wallet_request = {"amount":{"asset":"USD/2","amount":100}}; // \Formance\Model\CreditWalletRequest

try {
    $apiInstance->creditWallet($id, $credit_wallet_request);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->creditWallet: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**|  | |
| **credit_wallet_request** | [**\Formance\Model\CreditWalletRequest**](../Model/CreditWalletRequest.md)|  | [optional] |

### Return type

void (empty response body)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `debitWallet()`

```php
debitWallet($id, $debit_wallet_request): \Formance\Model\DebitWalletResponse
```

Debit a wallet

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 'id_example'; // string
$debit_wallet_request = new \Formance\Model\DebitWalletRequest(); // \Formance\Model\DebitWalletRequest

try {
    $result = $apiInstance->debitWallet($id, $debit_wallet_request);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->debitWallet: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**|  | |
| **debit_wallet_request** | [**\Formance\Model\DebitWalletRequest**](../Model/DebitWalletRequest.md)|  | [optional] |

### Return type

[**\Formance\Model\DebitWalletResponse**](../Model/DebitWalletResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `getBalance()`

```php
getBalance($id, $balance_name): \Formance\Model\GetBalanceResponse
```

Get detailed balance

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 'id_example'; // string
$balance_name = 'balance_name_example'; // string

try {
    $result = $apiInstance->getBalance($id, $balance_name);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->getBalance: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**|  | |
| **balance_name** | **string**|  | |

### Return type

[**\Formance\Model\GetBalanceResponse**](../Model/GetBalanceResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `getHold()`

```php
getHold($hold_id): \Formance\Model\GetHoldResponse
```

Get a hold

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$hold_id = 'hold_id_example'; // string | The hold ID

try {
    $result = $apiInstance->getHold($hold_id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->getHold: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **hold_id** | **string**| The hold ID | |

### Return type

[**\Formance\Model\GetHoldResponse**](../Model/GetHoldResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `getHolds()`

```php
getHolds($page_size, $wallet_id, $metadata, $cursor): \Formance\Model\GetHoldsResponse
```

Get all holds for a wallet

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$page_size = 100; // int | The maximum number of results to return per page
$wallet_id = wallet1; // string | The wallet to filter on
$metadata = metadata[key]=value1&metadata[a.nested.key]=value2; // object | Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below.
$cursor = aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==; // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.

try {
    $result = $apiInstance->getHolds($page_size, $wallet_id, $metadata, $cursor);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->getHolds: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **page_size** | **int**| The maximum number of results to return per page | [optional] [default to 15] |
| **wallet_id** | **string**| The wallet to filter on | [optional] |
| **metadata** | [**object**](../Model/.md)| Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below. | [optional] |
| **cursor** | **string**| Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set. | [optional] |

### Return type

[**\Formance\Model\GetHoldsResponse**](../Model/GetHoldsResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `getTransactions()`

```php
getTransactions($page_size, $wallet_id, $cursor): \Formance\Model\GetTransactionsResponse
```



### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$page_size = 100; // int | The maximum number of results to return per page
$wallet_id = wallet1; // string | A wallet ID to filter on
$cursor = aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==; // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set.

try {
    $result = $apiInstance->getTransactions($page_size, $wallet_id, $cursor);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->getTransactions: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **page_size** | **int**| The maximum number of results to return per page | [optional] [default to 15] |
| **wallet_id** | **string**| A wallet ID to filter on | [optional] |
| **cursor** | **string**| Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set. | [optional] |

### Return type

[**\Formance\Model\GetTransactionsResponse**](../Model/GetTransactionsResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `getWallet()`

```php
getWallet($id): \Formance\Model\GetWalletResponse
```

Get a wallet

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 'id_example'; // string

try {
    $result = $apiInstance->getWallet($id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->getWallet: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**|  | |

### Return type

[**\Formance\Model\GetWalletResponse**](../Model/GetWalletResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `listBalances()`

```php
listBalances($id): \Formance\Model\ListBalancesResponse
```

List balances of a wallet

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 'id_example'; // string

try {
    $result = $apiInstance->listBalances($id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->listBalances: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**|  | |

### Return type

[**\Formance\Model\ListBalancesResponse**](../Model/ListBalancesResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `listWallets()`

```php
listWallets($name, $metadata, $page_size, $cursor): \Formance\Model\ListWalletsResponse
```

List all wallets

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$name = wallet1; // string | Filter on wallet name
$metadata = metadata[key]=value1&metadata[a.nested.key]=value2; // object | Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below.
$page_size = 100; // int | The maximum number of results to return per page
$cursor = aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==; // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.

try {
    $result = $apiInstance->listWallets($name, $metadata, $page_size, $cursor);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->listWallets: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **name** | **string**| Filter on wallet name | [optional] |
| **metadata** | [**object**](../Model/.md)| Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below. | [optional] |
| **page_size** | **int**| The maximum number of results to return per page | [optional] [default to 15] |
| **cursor** | **string**| Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set. | [optional] |

### Return type

[**\Formance\Model\ListWalletsResponse**](../Model/ListWalletsResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `updateWallet()`

```php
updateWallet($id, $update_wallet_request)
```

Update a wallet

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 'id_example'; // string
$update_wallet_request = new \Formance\Model\UpdateWalletRequest(); // \Formance\Model\UpdateWalletRequest

try {
    $apiInstance->updateWallet($id, $update_wallet_request);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->updateWallet: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**|  | |
| **update_wallet_request** | [**\Formance\Model\UpdateWalletRequest**](../Model/UpdateWalletRequest.md)|  | [optional] |

### Return type

void (empty response body)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `voidHold()`

```php
voidHold($hold_id)
```

Cancel a hold

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$hold_id = 'hold_id_example'; // string

try {
    $apiInstance->voidHold($hold_id);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->voidHold: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **hold_id** | **string**|  | |

### Return type

void (empty response body)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `walletsgetServerInfo()`

```php
walletsgetServerInfo(): \Formance\Model\ServerInfo
```

Get server info

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WalletsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);

try {
    $result = $apiInstance->walletsgetServerInfo();
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WalletsApi->walletsgetServerInfo: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**\Formance\Model\ServerInfo**](../Model/ServerInfo.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
