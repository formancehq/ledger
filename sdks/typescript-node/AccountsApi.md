# formance.AccountsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**addMetadataToAccount**](AccountsApi.md#addMetadataToAccount) | **POST** /api/ledger/{ledger}/accounts/{address}/metadata | Add metadata to an account
[**countAccounts**](AccountsApi.md#countAccounts) | **HEAD** /api/ledger/{ledger}/accounts | Count the accounts from a ledger
[**getAccount**](AccountsApi.md#getAccount) | **GET** /api/ledger/{ledger}/accounts/{address} | Get account by its address
[**listAccounts**](AccountsApi.md#listAccounts) | **GET** /api/ledger/{ledger}/accounts | List accounts from a ledger


# **addMetadataToAccount**
> void addMetadataToAccount(requestBody)


### Example


```typescript
import { AccountsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new AccountsApi(configuration);

let body:AccountsApiAddMetadataToAccountRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // string | Exact address of the account. It must match the following regular expressions pattern: ``` ^\\w+(:\\w+)*$ ``` 
  address: "users:001",
  // { [key: string]: any; } | metadata
  requestBody: {
    "key": null,
  },
};

apiInstance.addMetadataToAccount(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **requestBody** | **{ [key: string]: any; }**| metadata |
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **address** | [**string**] | Exact address of the account. It must match the following regular expressions pattern: &#x60;&#x60;&#x60; ^\\w+(:\\w+)*$ &#x60;&#x60;&#x60;  | defaults to undefined


### Return type

**void**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | No Content |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **countAccounts**
> void countAccounts()


### Example


```typescript
import { AccountsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new AccountsApi(configuration);

let body:AccountsApiCountAccountsRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // string | Filter accounts by address pattern (regular expression placed between ^ and $). (optional)
  address: "users:.+",
  // any | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
  metadata: {},
};

apiInstance.countAccounts(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **address** | [**string**] | Filter accounts by address pattern (regular expression placed between ^ and $). | (optional) defaults to undefined
 **metadata** | **any** | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. | (optional) defaults to undefined


### Return type

**void**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | OK |  * Count -  <br>  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **getAccount**
> AccountResponse getAccount()


### Example


```typescript
import { AccountsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new AccountsApi(configuration);

let body:AccountsApiGetAccountRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // string | Exact address of the account. It must match the following regular expressions pattern: ``` ^\\w+(:\\w+)*$ ``` 
  address: "users:001",
};

apiInstance.getAccount(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **address** | [**string**] | Exact address of the account. It must match the following regular expressions pattern: &#x60;&#x60;&#x60; ^\\w+(:\\w+)*$ &#x60;&#x60;&#x60;  | defaults to undefined


### Return type

**AccountResponse**

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

# **listAccounts**
> AccountsCursorResponse listAccounts()

List accounts from a ledger, sorted by address in descending order.

### Example


```typescript
import { AccountsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new AccountsApi(configuration);

let body:AccountsApiListAccountsRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // number | The maximum number of results to return per page.  (optional)
  pageSize: 100,
  // number | The maximum number of results to return per page. Deprecated, please use `pageSize` instead.  (optional)
  pageSize2: 100,
  // string | Pagination cursor, will return accounts after given address, in descending order. (optional)
  after: "users:003",
  // string | Filter accounts by address pattern (regular expression placed between ^ and $). (optional)
  address: "users:.+",
  // any | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
  metadata: {},
  // number | Filter accounts by their balance (default operator is gte) (optional)
  balance: 2400,
  // 'gte' | 'lte' | 'gt' | 'lt' | 'e' | 'ne' | Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not.  (optional)
  balanceOperator: "gte",
  // 'gte' | 'lte' | 'gt' | 'lt' | 'e' | 'ne' | Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not. Deprecated, please use `balanceOperator` instead.  (optional)
  balanceOperator2: "gte",
  // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
  cursor: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
  // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use `cursor` instead.  (optional)
  paginationToken: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
};

apiInstance.listAccounts(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **pageSize** | [**number**] | The maximum number of results to return per page.  | (optional) defaults to 15
 **pageSize2** | [**number**] | The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead.  | (optional) defaults to 15
 **after** | [**string**] | Pagination cursor, will return accounts after given address, in descending order. | (optional) defaults to undefined
 **address** | [**string**] | Filter accounts by address pattern (regular expression placed between ^ and $). | (optional) defaults to undefined
 **metadata** | **any** | Filter accounts by metadata key value pairs. Nested objects can be used as seen in the example below. | (optional) defaults to undefined
 **balance** | [**number**] | Filter accounts by their balance (default operator is gte) | (optional) defaults to undefined
 **balanceOperator** | [**&#39;gte&#39; | &#39;lte&#39; | &#39;gt&#39; | &#39;lt&#39; | &#39;e&#39; | &#39;ne&#39;**]**Array<&#39;gte&#39; &#124; &#39;lte&#39; &#124; &#39;gt&#39; &#124; &#39;lt&#39; &#124; &#39;e&#39; &#124; &#39;ne&#39; &#124; &#39;11184809&#39;>** | Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not.  | (optional) defaults to undefined
 **balanceOperator2** | [**&#39;gte&#39; | &#39;lte&#39; | &#39;gt&#39; | &#39;lt&#39; | &#39;e&#39; | &#39;ne&#39;**]**Array<&#39;gte&#39; &#124; &#39;lte&#39; &#124; &#39;gt&#39; &#124; &#39;lt&#39; &#124; &#39;e&#39; &#124; &#39;ne&#39; &#124; &#39;11184809&#39;>** | Operator used for the filtering of balances can be greater than/equal, less than/equal, greater than, less than, equal or not. Deprecated, please use &#x60;balanceOperator&#x60; instead.  | (optional) defaults to undefined
 **cursor** | [**string**] | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | (optional) defaults to undefined
 **paginationToken** | [**string**] | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead.  | (optional) defaults to undefined


### Return type

**AccountsCursorResponse**

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

