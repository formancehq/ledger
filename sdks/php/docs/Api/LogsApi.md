# Formance\LogsApi

All URIs are relative to http://localhost, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**listLogs()**](LogsApi.md#listLogs) | **GET** /api/ledger/{ledger}/log | List the logs from a ledger |


## `listLogs()`

```php
listLogs($ledger, $page_size, $page_size2, $after, $start_time, $start_time2, $end_time, $end_time2, $cursor, $pagination_token): \Formance\Model\LogsCursorResponse
```

List the logs from a ledger

List the logs from a ledger, sorted by ID in descending order.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');


// Configure OAuth2 access token for authorization: Authorization
$config = Formance\Configuration::getDefaultConfiguration()->setAccessToken('YOUR_ACCESS_TOKEN');


$apiInstance = new Formance\Api\LogsApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client(),
    $config
);
$ledger = ledger001; // string | Name of the ledger.
$page_size = 100; // int | The maximum number of results to return per page.
$page_size2 = 100; // int | The maximum number of results to return per page. Deprecated, please use `pageSize` instead.
$after = 1234; // string | Pagination cursor, will return the logs after a given ID. (in descending order).
$start_time = new \DateTime("2013-10-20T19:20:30+01:00"); // \DateTime | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \"2023-01-02T15:04:01Z\" includes the first second of 4th minute).
$start_time2 = new \DateTime("2013-10-20T19:20:30+01:00"); // \DateTime | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \"2023-01-02T15:04:01Z\" includes the first second of 4th minute). Deprecated, please use `startTime` instead.
$end_time = new \DateTime("2013-10-20T19:20:30+01:00"); // \DateTime | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \"2023-01-02T15:04:01Z\" excludes the first second of 4th minute).
$end_time2 = new \DateTime("2013-10-20T19:20:30+01:00"); // \DateTime | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \"2023-01-02T15:04:01Z\" excludes the first second of 4th minute). Deprecated, please use `endTime` instead.
$cursor = aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==; // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.
$pagination_token = aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==; // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use `cursor` instead.

try {
    $result = $apiInstance->listLogs($ledger, $page_size, $page_size2, $after, $start_time, $start_time2, $end_time, $end_time2, $cursor, $pagination_token);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling LogsApi->listLogs: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **ledger** | **string**| Name of the ledger. | |
| **page_size** | **int**| The maximum number of results to return per page. | [optional] [default to 15] |
| **page_size2** | **int**| The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead. | [optional] [default to 15] |
| **after** | **string**| Pagination cursor, will return the logs after a given ID. (in descending order). | [optional] |
| **start_time** | **\DateTime**| Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). | [optional] |
| **start_time2** | **\DateTime**| Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead. | [optional] |
| **end_time** | **\DateTime**| Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). | [optional] |
| **end_time2** | **\DateTime**| Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead. | [optional] |
| **cursor** | **string**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. | [optional] |
| **pagination_token** | **string**| Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead. | [optional] |

### Return type

[**\Formance\Model\LogsCursorResponse**](../Model/LogsCursorResponse.md)

### Authorization

[Authorization](../../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
