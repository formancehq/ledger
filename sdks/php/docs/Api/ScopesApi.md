# Formance\ScopesApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**addTransientScope()**](ScopesApi.md#addTransientScope) | **PUT** /api/auth/scopes/{scopeId}/transient/{transientScopeId} | Add a transient scope to a scope |
| [**createScope()**](ScopesApi.md#createScope) | **POST** /api/auth/scopes | Create scope |
| [**deleteScope()**](ScopesApi.md#deleteScope) | **DELETE** /api/auth/scopes/{scopeId} | Delete scope |
| [**deleteTransientScope()**](ScopesApi.md#deleteTransientScope) | **DELETE** /api/auth/scopes/{scopeId}/transient/{transientScopeId} | Delete a transient scope from a scope |
| [**listScopes()**](ScopesApi.md#listScopes) | **GET** /api/auth/scopes | List scopes |
| [**readScope()**](ScopesApi.md#readScope) | **GET** /api/auth/scopes/{scopeId} | Read scope |
| [**updateScope()**](ScopesApi.md#updateScope) | **PUT** /api/auth/scopes/{scopeId} | Update scope |


## `addTransientScope()`

```php
addTransientScope($scope_id, $transient_scope_id)
```

Add a transient scope to a scope

Add a transient scope to a scope

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ScopesApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$scope_id = 'scope_id_example'; // string | Scope ID
$transient_scope_id = 'transient_scope_id_example'; // string | Transient scope ID

try {
    $apiInstance->addTransientScope($scope_id, $transient_scope_id);
} catch (Exception $e) {
    echo 'Exception when calling ScopesApi->addTransientScope: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **scope_id** | **string**| Scope ID | |
| **transient_scope_id** | **string**| Transient scope ID | |

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

## `createScope()`

```php
createScope($body): \Formance\Model\CreateScopeResponse
```

Create scope

Create scope

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ScopesApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$body = new \Formance\Model\ScopeOptions(); // \Formance\Model\ScopeOptions

try {
    $result = $apiInstance->createScope($body);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling ScopesApi->createScope: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **body** | **\Formance\Model\ScopeOptions**|  | [optional] |

### Return type

[**\Formance\Model\CreateScopeResponse**](../Model/CreateScopeResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `deleteScope()`

```php
deleteScope($scope_id)
```

Delete scope

Delete scope

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ScopesApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$scope_id = 'scope_id_example'; // string | Scope ID

try {
    $apiInstance->deleteScope($scope_id);
} catch (Exception $e) {
    echo 'Exception when calling ScopesApi->deleteScope: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **scope_id** | **string**| Scope ID | |

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

## `deleteTransientScope()`

```php
deleteTransientScope($scope_id, $transient_scope_id)
```

Delete a transient scope from a scope

Delete a transient scope from a scope

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ScopesApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$scope_id = 'scope_id_example'; // string | Scope ID
$transient_scope_id = 'transient_scope_id_example'; // string | Transient scope ID

try {
    $apiInstance->deleteTransientScope($scope_id, $transient_scope_id);
} catch (Exception $e) {
    echo 'Exception when calling ScopesApi->deleteTransientScope: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **scope_id** | **string**| Scope ID | |
| **transient_scope_id** | **string**| Transient scope ID | |

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

## `listScopes()`

```php
listScopes(): \Formance\Model\ListScopesResponse
```

List scopes

List Scopes

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ScopesApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);

try {
    $result = $apiInstance->listScopes();
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling ScopesApi->listScopes: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**\Formance\Model\ListScopesResponse**](../Model/ListScopesResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `readScope()`

```php
readScope($scope_id): \Formance\Model\CreateScopeResponse
```

Read scope

Read scope

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ScopesApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$scope_id = 'scope_id_example'; // string | Scope ID

try {
    $result = $apiInstance->readScope($scope_id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling ScopesApi->readScope: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **scope_id** | **string**| Scope ID | |

### Return type

[**\Formance\Model\CreateScopeResponse**](../Model/CreateScopeResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `updateScope()`

```php
updateScope($scope_id, $body): \Formance\Model\CreateScopeResponse
```

Update scope

Update scope

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ScopesApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$scope_id = 'scope_id_example'; // string | Scope ID
$body = new \Formance\Model\ScopeOptions(); // \Formance\Model\ScopeOptions

try {
    $result = $apiInstance->updateScope($scope_id, $body);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling ScopesApi->updateScope: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **scope_id** | **string**| Scope ID | |
| **body** | **\Formance\Model\ScopeOptions**|  | [optional] |

### Return type

[**\Formance\Model\CreateScopeResponse**](../Model/CreateScopeResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
