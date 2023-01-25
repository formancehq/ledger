# Formance\WebhooksApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**activateConfig()**](WebhooksApi.md#activateConfig) | **PUT** /api/webhooks/configs/{id}/activate | Activate one config |
| [**changeConfigSecret()**](WebhooksApi.md#changeConfigSecret) | **PUT** /api/webhooks/configs/{id}/secret/change | Change the signing secret of a config |
| [**deactivateConfig()**](WebhooksApi.md#deactivateConfig) | **PUT** /api/webhooks/configs/{id}/deactivate | Deactivate one config |
| [**deleteConfig()**](WebhooksApi.md#deleteConfig) | **DELETE** /api/webhooks/configs/{id} | Delete one config |
| [**getManyConfigs()**](WebhooksApi.md#getManyConfigs) | **GET** /api/webhooks/configs | Get many configs |
| [**insertConfig()**](WebhooksApi.md#insertConfig) | **POST** /api/webhooks/configs | Insert a new config |
| [**testConfig()**](WebhooksApi.md#testConfig) | **GET** /api/webhooks/configs/{id}/test | Test one config |


## `activateConfig()`

```php
activateConfig($id): \Formance\Model\ConfigResponse
```

Activate one config

Activate a webhooks config by ID, to start receiving webhooks to its endpoint.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WebhooksApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 4997257d-dfb6-445b-929c-cbe2ab182818; // string | Config ID

try {
    $result = $apiInstance->activateConfig($id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WebhooksApi->activateConfig: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**| Config ID | |

### Return type

[**\Formance\Model\ConfigResponse**](../Model/ConfigResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `changeConfigSecret()`

```php
changeConfigSecret($id, $config_change_secret): \Formance\Model\ConfigResponse
```

Change the signing secret of a config

Change the signing secret of the endpoint of a webhooks config.  If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding)

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WebhooksApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 4997257d-dfb6-445b-929c-cbe2ab182818; // string | Config ID
$config_change_secret = new \Formance\Model\ConfigChangeSecret(); // \Formance\Model\ConfigChangeSecret

try {
    $result = $apiInstance->changeConfigSecret($id, $config_change_secret);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WebhooksApi->changeConfigSecret: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**| Config ID | |
| **config_change_secret** | [**\Formance\Model\ConfigChangeSecret**](../Model/ConfigChangeSecret.md)|  | [optional] |

### Return type

[**\Formance\Model\ConfigResponse**](../Model/ConfigResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `deactivateConfig()`

```php
deactivateConfig($id): \Formance\Model\ConfigResponse
```

Deactivate one config

Deactivate a webhooks config by ID, to stop receiving webhooks to its endpoint.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WebhooksApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 4997257d-dfb6-445b-929c-cbe2ab182818; // string | Config ID

try {
    $result = $apiInstance->deactivateConfig($id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WebhooksApi->deactivateConfig: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**| Config ID | |

### Return type

[**\Formance\Model\ConfigResponse**](../Model/ConfigResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `deleteConfig()`

```php
deleteConfig($id)
```

Delete one config

Delete a webhooks config by ID.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WebhooksApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 4997257d-dfb6-445b-929c-cbe2ab182818; // string | Config ID

try {
    $apiInstance->deleteConfig($id);
} catch (Exception $e) {
    echo 'Exception when calling WebhooksApi->deleteConfig: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**| Config ID | |

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

## `getManyConfigs()`

```php
getManyConfigs($id, $endpoint): \Formance\Model\ConfigsResponse
```

Get many configs

Sorted by updated date descending

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WebhooksApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 4997257d-dfb6-445b-929c-cbe2ab182818; // string | Optional filter by Config ID
$endpoint = https://example.com; // string | Optional filter by endpoint URL

try {
    $result = $apiInstance->getManyConfigs($id, $endpoint);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WebhooksApi->getManyConfigs: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**| Optional filter by Config ID | [optional] |
| **endpoint** | **string**| Optional filter by endpoint URL | [optional] |

### Return type

[**\Formance\Model\ConfigsResponse**](../Model/ConfigsResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `insertConfig()`

```php
insertConfig($config_user): \Formance\Model\ConfigResponse
```

Insert a new config

Insert a new webhooks config.  The endpoint should be a valid https URL and be unique.  The secret is the endpoint's verification secret. If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding)  All eventTypes are converted to lower-case when inserted.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WebhooksApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$config_user = new \Formance\Model\ConfigUser(); // \Formance\Model\ConfigUser

try {
    $result = $apiInstance->insertConfig($config_user);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WebhooksApi->insertConfig: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **config_user** | [**\Formance\Model\ConfigUser**](../Model/ConfigUser.md)|  | |

### Return type

[**\Formance\Model\ConfigResponse**](../Model/ConfigResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`, `text/plain`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `testConfig()`

```php
testConfig($id): \Formance\Model\AttemptResponse
```

Test one config

Test a config by sending a webhook to its endpoint.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\WebhooksApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$id = 4997257d-dfb6-445b-929c-cbe2ab182818; // string | Config ID

try {
    $result = $apiInstance->testConfig($id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling WebhooksApi->testConfig: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **id** | **string**| Config ID | |

### Return type

[**\Formance\Model\AttemptResponse**](../Model/AttemptResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
