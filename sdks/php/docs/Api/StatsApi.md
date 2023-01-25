# Formance\StatsApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**readStats()**](StatsApi.md#readStats) | **GET** /api/ledger/{ledger}/stats | Get statistics from a ledger |


## `readStats()`

```php
readStats($ledger): \Formance\Model\StatsResponse
```

Get statistics from a ledger

Get statistics from a ledger. (aggregate metrics on accounts and transactions)

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\StatsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$ledger = ledger001; // string | name of the ledger

try {
    $result = $apiInstance->readStats($ledger);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling StatsApi->readStats: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **ledger** | **string**| name of the ledger | |

### Return type

[**\Formance\Model\StatsResponse**](../Model/StatsResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
