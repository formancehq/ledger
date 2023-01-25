# formance.LogsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**listLogs**](LogsApi.md#listLogs) | **GET** /api/ledger/{ledger}/log | List the logs from a ledger


# **listLogs**
> LogsCursorResponse listLogs()

List the logs from a ledger, sorted by ID in descending order.

### Example


```typescript
import { LogsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new LogsApi(configuration);

let body:LogsApiListLogsRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // number | The maximum number of results to return per page.  (optional)
  pageSize: 100,
  // number | The maximum number of results to return per page. Deprecated, please use `pageSize` instead.  (optional)
  pageSize2: 100,
  // string | Pagination cursor, will return the logs after a given ID. (in descending order). (optional)
  after: "1234",
  // Date | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \"2023-01-02T15:04:01Z\" includes the first second of 4th minute).  (optional)
  startTime: new Date('1970-01-01T00:00:00.00Z'),
  // Date | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \"2023-01-02T15:04:01Z\" includes the first second of 4th minute). Deprecated, please use `startTime` instead.  (optional)
  startTime2: new Date('1970-01-01T00:00:00.00Z'),
  // Date | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \"2023-01-02T15:04:01Z\" excludes the first second of 4th minute).  (optional)
  endTime: new Date('1970-01-01T00:00:00.00Z'),
  // Date | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \"2023-01-02T15:04:01Z\" excludes the first second of 4th minute). Deprecated, please use `endTime` instead.  (optional)
  endTime2: new Date('1970-01-01T00:00:00.00Z'),
  // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
  cursor: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
  // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use `cursor` instead.  (optional)
  paginationToken: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
};

apiInstance.listLogs(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **pageSize** | [**number**] | The maximum number of results to return per page.  | (optional) defaults to 15
 **pageSize2** | [**number**] | The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead.  | (optional) defaults to 15
 **after** | [**string**] | Pagination cursor, will return the logs after a given ID. (in descending order). | (optional) defaults to undefined
 **startTime** | [**Date**] | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute).  | (optional) defaults to undefined
 **startTime2** | [**Date**] | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead.  | (optional) defaults to undefined
 **endTime** | [**Date**] | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute).  | (optional) defaults to undefined
 **endTime2** | [**Date**] | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead.  | (optional) defaults to undefined
 **cursor** | [**string**] | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | (optional) defaults to undefined
 **paginationToken** | [**string**] | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead.  | (optional) defaults to undefined


### Return type

**LogsCursorResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | OK |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

