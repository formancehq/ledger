# formance.LedgerApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getLedgerInfo**](LedgerApi.md#getLedgerInfo) | **GET** /api/ledger/{ledger}/_info | Get information about a ledger


# **getLedgerInfo**
> LedgerInfoResponse getLedgerInfo()


### Example


```typescript
import { LedgerApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new LedgerApi(configuration);

let body:LedgerApiGetLedgerInfoRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
};

apiInstance.getLedgerInfo(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined


### Return type

**LedgerInfoResponse**

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

