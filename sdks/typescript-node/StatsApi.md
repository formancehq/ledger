# formance.StatsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**readStats**](StatsApi.md#readStats) | **GET** /api/ledger/{ledger}/stats | Get statistics from a ledger


# **readStats**
> StatsResponse readStats()

Get statistics from a ledger. (aggregate metrics on accounts and transactions) 

### Example


```typescript
import { StatsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new StatsApi(configuration);

let body:StatsApiReadStatsRequest = {
  // string | name of the ledger
  ledger: "ledger001",
};

apiInstance.readStats(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | name of the ledger | defaults to undefined


### Return type

**StatsResponse**

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

