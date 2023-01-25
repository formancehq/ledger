# Formance\SearchApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**search()**](SearchApi.md#search) | **POST** /api/search/ | Search |


## `search()`

```php
search($query): \Formance\Model\Response
```

Search

ElasticSearch query engine

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\SearchApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$query = new \Formance\Model\Query(); // \Formance\Model\Query

try {
    $result = $apiInstance->search($query);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling SearchApi->search: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **query** | [**\Formance\Model\Query**](../Model/Query.md)|  | |

### Return type

[**\Formance\Model\Response**](../Model/Response.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
