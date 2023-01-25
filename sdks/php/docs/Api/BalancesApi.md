# Formance\BalancesApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**getBalances()**](BalancesApi.md#getBalances) | **GET** /api/ledger/{ledger}/balances | Get the balances from a ledger&#39;s account |
| [**getBalancesAggregated()**](BalancesApi.md#getBalancesAggregated) | **GET** /api/ledger/{ledger}/aggregate/balances | Get the aggregated balances from selected accounts |


## `getBalances()`

```php
getBalances($ledger, $address, $after, $cursor, $pagination_token): \Formance\Model\BalancesCursorResponse
```

Get the balances from a ledger's account

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\BalancesApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$ledger = ledger001; // string | Name of the ledger.
$address = users:001; // string | Filter balances involving given account, either as source or destination.
$after = users:003; // string | Pagination cursor, will return accounts after given address, in descending order.
$cursor = aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==; // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.
$pagination_token = aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==; // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. Deprecated, please use `cursor` instead.

try {
    $result = $apiInstance->getBalances($ledger, $address, $after, $cursor, $pagination_token);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling BalancesApi->getBalances: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **ledger** | **string**| Name of the ledger. | |
| **address** | **string**| Filter balances involving given account, either as source or destination. | [optional] |
| **after** | **string**| Pagination cursor, will return accounts after given address, in descending order. | [optional] |
| **cursor** | **string**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. | [optional] |
| **pagination_token** | **string**| Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. Deprecated, please use &#x60;cursor&#x60; instead. | [optional] |

### Return type

[**\Formance\Model\BalancesCursorResponse**](../Model/BalancesCursorResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `getBalancesAggregated()`

```php
getBalancesAggregated($ledger, $address): \Formance\Model\AggregateBalancesResponse
```

Get the aggregated balances from selected accounts

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\BalancesApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$ledger = ledger001; // string | Name of the ledger.
$address = users:001; // string | Filter balances involving given account, either as source or destination.

try {
    $result = $apiInstance->getBalancesAggregated($ledger, $address);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling BalancesApi->getBalancesAggregated: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **ledger** | **string**| Name of the ledger. | |
| **address** | **string**| Filter balances involving given account, either as source or destination. | [optional] |

### Return type

[**\Formance\Model\AggregateBalancesResponse**](../Model/AggregateBalancesResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
