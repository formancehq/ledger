# Formance\OrchestrationApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**createWorkflow()**](OrchestrationApi.md#createWorkflow) | **POST** /api/orchestration/flows | Create workflow |
| [**getFlow()**](OrchestrationApi.md#getFlow) | **GET** /api/orchestration/flows/{flowId} | Get a flow by id |
| [**getWorkflowOccurrence()**](OrchestrationApi.md#getWorkflowOccurrence) | **GET** /api/orchestration/flows/{flowId}/runs/{runId} | Get a workflow occurrence by id |
| [**listFlows()**](OrchestrationApi.md#listFlows) | **GET** /api/orchestration/flows | List registered flows |
| [**listRuns()**](OrchestrationApi.md#listRuns) | **GET** /api/orchestration/flows/{flowId}/runs | List occurrences of a workflow |
| [**orchestrationgetServerInfo()**](OrchestrationApi.md#orchestrationgetServerInfo) | **GET** /api/orchestration/_info | Get server info |
| [**runWorkflow()**](OrchestrationApi.md#runWorkflow) | **POST** /api/orchestration/flows/{flowId}/runs | Run workflow |


## `createWorkflow()`

```php
createWorkflow($body): \Formance\Model\CreateWorkflowResponse
```

Create workflow

Create a workflow

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\OrchestrationApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$body = new \Formance\Model\WorkflowConfig(); // \Formance\Model\WorkflowConfig

try {
    $result = $apiInstance->createWorkflow($body);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling OrchestrationApi->createWorkflow: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **body** | **\Formance\Model\WorkflowConfig**|  | [optional] |

### Return type

[**\Formance\Model\CreateWorkflowResponse**](../Model/CreateWorkflowResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `getFlow()`

```php
getFlow($flow_id): \Formance\Model\GetWorkflowResponse
```

Get a flow by id

Get a flow by id

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\OrchestrationApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$flow_id = xxx; // string | The flow id

try {
    $result = $apiInstance->getFlow($flow_id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling OrchestrationApi->getFlow: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **flow_id** | **string**| The flow id | |

### Return type

[**\Formance\Model\GetWorkflowResponse**](../Model/GetWorkflowResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `getWorkflowOccurrence()`

```php
getWorkflowOccurrence($flow_id, $run_id): \Formance\Model\GetWorkflowOccurrenceResponse
```

Get a workflow occurrence by id

Get a workflow occurrence by id

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\OrchestrationApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$flow_id = xxx; // string | The flow id
$run_id = xxx; // string | The occurrence id

try {
    $result = $apiInstance->getWorkflowOccurrence($flow_id, $run_id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling OrchestrationApi->getWorkflowOccurrence: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **flow_id** | **string**| The flow id | |
| **run_id** | **string**| The occurrence id | |

### Return type

[**\Formance\Model\GetWorkflowOccurrenceResponse**](../Model/GetWorkflowOccurrenceResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `listFlows()`

```php
listFlows(): \Formance\Model\ListWorkflowsResponse
```

List registered flows

List registered flows

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\OrchestrationApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);

try {
    $result = $apiInstance->listFlows();
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling OrchestrationApi->listFlows: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**\Formance\Model\ListWorkflowsResponse**](../Model/ListWorkflowsResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `listRuns()`

```php
listRuns($flow_id): \Formance\Model\ListRunsResponse
```

List occurrences of a workflow

List occurrences of a workflow

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\OrchestrationApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$flow_id = xxx; // string | The flow id

try {
    $result = $apiInstance->listRuns($flow_id);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling OrchestrationApi->listRuns: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **flow_id** | **string**| The flow id | |

### Return type

[**\Formance\Model\ListRunsResponse**](../Model/ListRunsResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `orchestrationgetServerInfo()`

```php
orchestrationgetServerInfo(): \Formance\Model\ServerInfo
```

Get server info

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\OrchestrationApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);

try {
    $result = $apiInstance->orchestrationgetServerInfo();
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling OrchestrationApi->orchestrationgetServerInfo: ', $e->getMessage(), PHP_EOL;
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

## `runWorkflow()`

```php
runWorkflow($flow_id, $wait, $request_body): \Formance\Model\RunWorkflowResponse
```

Run workflow

Run workflow

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\OrchestrationApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$flow_id = xxx; // string | The flow id
$wait = True; // bool | Wait end of the workflow before return
$request_body = array('key' => 'request_body_example'); // array<string,string>

try {
    $result = $apiInstance->runWorkflow($flow_id, $wait, $request_body);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling OrchestrationApi->runWorkflow: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **flow_id** | **string**| The flow id | |
| **wait** | **bool**| Wait end of the workflow before return | [optional] |
| **request_body** | [**array<string,string>**](../Model/string.md)|  | [optional] |

### Return type

[**\Formance\Model\RunWorkflowResponse**](../Model/RunWorkflowResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: `application/json`
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
