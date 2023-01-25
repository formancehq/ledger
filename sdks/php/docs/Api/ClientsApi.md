# Formance\ClientsApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**addScopeToClient()**](ClientsApi.md#addScopeToClient) | **PUT** /api/auth/clients/{clientId}/scopes/{scopeId} | Add scope to client |
| [**createClient()**](ClientsApi.md#createClient) | **POST** /api/auth/clients | Create client |
| [**createSecret()**](ClientsApi.md#createSecret) | **POST** /api/auth/clients/{clientId}/secrets | Add a secret to a client |
| [**deleteClient()**](ClientsApi.md#deleteClient) | **DELETE** /api/auth/clients/{clientId} | Delete client |
| [**deleteScopeFromClient()**](ClientsApi.md#deleteScopeFromClient) | **DELETE** /api/auth/clients/{clientId}/scopes/{scopeId} | Delete scope from client |
| [**deleteSecret()**](ClientsApi.md#deleteSecret) | **DELETE** /api/auth/clients/{clientId}/secrets/{secretId} | Delete a secret from a client |
| [**listClients()**](ClientsApi.md#listClients) | **GET** /api/auth/clients | List clients |
| [**readClient()**](ClientsApi.md#readClient) | **GET** /api/auth/clients/{clientId} | Read client |
| [**updateClient()**](ClientsApi.md#updateClient) | **PUT** /api/auth/clients/{clientId} | Update client |


## `addScopeToClient()`

```php
addScopeToClient($client_id, $scope_id)
```

Add scope to client

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ClientsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$client_id = 'client_id_example'; // string | Client ID
$scope_id = 'scope_id_example'; // string | Scope ID

try {
    $apiInstance->addScopeToClient($client_id, $scope_id);
} catch (Exception $e) {
    echo 'Exception when calling ClientsApi->addScopeToClient: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **client_id** | **string**| Client ID | |
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

## `createClient()`

```php
createClient($body): \Formance\Model\CreateClientResponse
```

Create client

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ClientsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$body = new \Formance\Model\ClientOptions(); // \Formance\Model\ClientOptions

try {
    $result = $apiInstance->createClient($body);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling ClientsApi->createClient: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **body** | **\Formance\Model\ClientOptions**|  | [optional] |

### Return type

[**\Formance\Model\CreateClientResponse**](../Model/CreateClientResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `createSecret()`

```php
createSecret($client_id, $body): \Formance\Model\CreateSecretResponse
```

Add a secret to a client

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ClientsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$client_id = 'client_id_example'; // string | Client ID
$body = new \Formance\Model\SecretOptions(); // \Formance\Model\SecretOptions

try {
    $result = $apiInstance->createSecret($client_id, $body);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling ClientsApi->createSecret: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **client_id** | **string**| Client ID | |
| **body** | **\Formance\Model\SecretOptions**|  | [optional] |

### Return type

[**\Formance\Model\CreateSecretResponse**](../Model/CreateSecretResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `deleteClient()`

```php
deleteClient($client_id)
```

Delete client

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ClientsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$client_id = 'client_id_example'; // string | Client ID

try {
    $apiInstance->deleteClient($client_id);
} catch (Exception $e) {
    echo 'Exception when calling ClientsApi->deleteClient: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **client_id** | **string**| Client ID | |

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

## `deleteScopeFromClient()`

```php
deleteScopeFromClient($client_id, $scope_id)
```

Delete scope from client

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ClientsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$client_id = 'client_id_example'; // string | Client ID
$scope_id = 'scope_id_example'; // string | Scope ID

try {
    $apiInstance->deleteScopeFromClient($client_id, $scope_id);
} catch (Exception $e) {
    echo 'Exception when calling ClientsApi->deleteScopeFromClient: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **client_id** | **string**| Client ID | |
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

## `deleteSecret()`

```php
deleteSecret($client_id, $secret_id)
```

Delete a secret from a client

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ClientsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$client_id = 'client_id_example'; // string | Client ID
$secret_id = 'secret_id_example'; // string | Secret ID

try {
    $apiInstance->deleteSecret($client_id, $secret_id);
} catch (Exception $e) {
    echo 'Exception when calling ClientsApi->deleteSecret: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **client_id** | **string**| Client ID | |
| **secret_id** | **string**| Secret ID | |

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

## `listClients()`

```php
listClients(): \Formance\Model\ListClientsResponse
```

List clients

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ClientsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);

try {
    $result = $apiInstance->listClients();
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling ClientsApi->listClients: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**\Formance\Model\ListClientsResponse**](../Model/ListClientsResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `readClient()`

```php
readClient($client_id): \Formance\Model\ReadClientResponse
```

Read client

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ClientsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$client_id = 'client_id_example'; // string | Client ID

try {
    $result = $apiInstance->readClient($client_id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling ClientsApi->readClient: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **client_id** | **string**| Client ID | |

### Return type

[**\Formance\Model\ReadClientResponse**](../Model/ReadClientResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `updateClient()`

```php
updateClient($client_id, $body): \Formance\Model\CreateClientResponse
```

Update client

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\ClientsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$client_id = 'client_id_example'; // string | Client ID
$body = new \Formance\Model\ClientOptions(); // \Formance\Model\ClientOptions

try {
    $result = $apiInstance->updateClient($client_id, $body);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling ClientsApi->updateClient: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **client_id** | **string**| Client ID | |
| **body** | **\Formance\Model\ClientOptions**|  | [optional] |

### Return type

[**\Formance\Model\CreateClientResponse**](../Model/CreateClientResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
