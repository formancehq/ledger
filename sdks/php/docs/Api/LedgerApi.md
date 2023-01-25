# Formance\LedgerApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**getLedgerInfo()**](LedgerApi.md#getLedgerInfo) | **GET** /api/ledger/{ledger}/_info | Get information about a ledger |


## `getLedgerInfo()`

```php
getLedgerInfo($ledger): \Formance\Model\LedgerInfoResponse
```

Get information about a ledger

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\LedgerApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$ledger = ledger001; // string | Name of the ledger.

try {
    $result = $apiInstance->getLedgerInfo($ledger);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling LedgerApi->getLedgerInfo: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **ledger** | **string**| Name of the ledger. | |

### Return type

[**\Formance\Model\LedgerInfoResponse**](../Model/LedgerInfoResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
