# formance.SearchApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**search**](SearchApi.md#search) | **POST** /api/search/ | Search


# **search**
> Response search(query)

ElasticSearch query engine

### Example


```typescript
import { SearchApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new SearchApi(configuration);

let body:SearchApiSearchRequest = {
  // Query
  query: {
    ledgers: [
      "quickstart",
    ],
    after: [
      "users:002",
    ],
    pageSize: 0,
    terms: [
      "destination=central_bank1",
    ],
    sort: "txid:asc",
    policy: "OR",
    target: "target_example",
    cursor: "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
  },
};

apiInstance.search(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **query** | **Query**|  |


### Return type

**Response**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Success |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

