# formance.BalancesApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**getBalances**](BalancesApi.md#getBalances) | **GET** /api/ledger/{ledger}/balances | Get the balances from a ledger&#39;s account
[**getBalancesAggregated**](BalancesApi.md#getBalancesAggregated) | **GET** /api/ledger/{ledger}/aggregate/balances | Get the aggregated balances from selected accounts


# **getBalances**
> BalancesCursorResponse getBalances()


### Example


```typescript
import { BalancesApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new BalancesApi(configuration);

let body:BalancesApiGetBalancesRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // string | Filter balances involving given account, either as source or destination. (optional)
  address: "users:001",
  // string | Pagination cursor, will return accounts after given address, in descending order. (optional)
  after: "users:003",
  // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
  cursor: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
  // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. Deprecated, please use `cursor` instead. (optional)
  paginationToken: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
};

apiInstance.getBalances(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **address** | [**string**] | Filter balances involving given account, either as source or destination. | (optional) defaults to undefined
 **after** | [**string**] | Pagination cursor, will return accounts after given address, in descending order. | (optional) defaults to undefined
 **cursor** | [**string**] | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | (optional) defaults to undefined
 **paginationToken** | [**string**] | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. Deprecated, please use &#x60;cursor&#x60; instead. | (optional) defaults to undefined


### Return type

**BalancesCursorResponse**

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

# **getBalancesAggregated**
> AggregateBalancesResponse getBalancesAggregated()


### Example


```typescript
import { BalancesApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new BalancesApi(configuration);

let body:BalancesApiGetBalancesAggregatedRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // string | Filter balances involving given account, either as source or destination. (optional)
  address: "users:001",
};

apiInstance.getBalancesAggregated(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **address** | [**string**] | Filter balances involving given account, either as source or destination. | (optional) defaults to undefined


### Return type

**AggregateBalancesResponse**

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

