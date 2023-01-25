# formance.MappingApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getMapping**](MappingApi.md#getMapping) | **GET** /api/ledger/{ledger}/mapping | Get the mapping of a ledger
[**updateMapping**](MappingApi.md#updateMapping) | **PUT** /api/ledger/{ledger}/mapping | Update the mapping of a ledger


# **getMapping**
> MappingResponse getMapping()


### Example


```typescript
import { MappingApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new MappingApi(configuration);

let body:MappingApiGetMappingRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
};

apiInstance.getMapping(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined


### Return type

**MappingResponse**

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

# **updateMapping**
> MappingResponse updateMapping(mapping)


### Example


```typescript
import { MappingApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new MappingApi(configuration);

let body:MappingApiUpdateMappingRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // Mapping
  mapping: {
    contracts: [
      {
        account: "users:001",
        expr: {},
      },
    ],
  },
};

apiInstance.updateMapping(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **mapping** | **Mapping**|  |
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined


### Return type

**MappingResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | OK |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

