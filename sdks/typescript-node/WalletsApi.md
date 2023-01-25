# formance.WalletsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**confirmHold**](WalletsApi.md#confirmHold) | **POST** /api/wallets/holds/{hold_id}/confirm | Confirm a hold
[**createBalance**](WalletsApi.md#createBalance) | **POST** /api/wallets/wallets/{id}/balances | Create a balance
[**createWallet**](WalletsApi.md#createWallet) | **POST** /api/wallets/wallets | Create a new wallet
[**creditWallet**](WalletsApi.md#creditWallet) | **POST** /api/wallets/wallets/{id}/credit | Credit a wallet
[**debitWallet**](WalletsApi.md#debitWallet) | **POST** /api/wallets/wallets/{id}/debit | Debit a wallet
[**getBalance**](WalletsApi.md#getBalance) | **GET** /api/wallets/wallets/{id}/balances/{balanceName} | Get detailed balance
[**getHold**](WalletsApi.md#getHold) | **GET** /api/wallets/holds/{holdID} | Get a hold
[**getHolds**](WalletsApi.md#getHolds) | **GET** /api/wallets/holds | Get all holds for a wallet
[**getTransactions**](WalletsApi.md#getTransactions) | **GET** /api/wallets/transactions | 
[**getWallet**](WalletsApi.md#getWallet) | **GET** /api/wallets/wallets/{id} | Get a wallet
[**listBalances**](WalletsApi.md#listBalances) | **GET** /api/wallets/wallets/{id}/balances | List balances of a wallet
[**listWallets**](WalletsApi.md#listWallets) | **GET** /api/wallets/wallets | List all wallets
[**updateWallet**](WalletsApi.md#updateWallet) | **PATCH** /api/wallets/wallets/{id} | Update a wallet
[**voidHold**](WalletsApi.md#voidHold) | **POST** /api/wallets/holds/{hold_id}/void | Cancel a hold
[**walletsgetServerInfo**](WalletsApi.md#walletsgetServerInfo) | **GET** /api/wallets/_info | Get server info


# **confirmHold**
> void confirmHold()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiConfirmHoldRequest = {
  // string
  holdId: "hold_id_example",
  // ConfirmHoldRequest (optional)
  confirmHoldRequest: {
    amount: 100,
    _final: true,
  },
};

apiInstance.confirmHold(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **confirmHoldRequest** | **ConfirmHoldRequest**|  |
 **holdId** | [**string**] |  | defaults to undefined


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
**204** | Hold successfully confirmed, funds moved back to initial destination |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **createBalance**
> CreateBalanceResponse createBalance()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiCreateBalanceRequest = {
  // string
  id: "id_example",
  // Balance (optional)
  body: {
    name: "name_example",
  },
};

apiInstance.createBalance(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **Balance**|  |
 **id** | [**string**] |  | defaults to undefined


### Return type

**CreateBalanceResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Created balance |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **createWallet**
> CreateWalletResponse createWallet()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiCreateWalletRequest = {
  // CreateWalletRequest (optional)
  createWalletRequest: {
    metadata: {
      "key": null,
    },
    name: "name_example",
  },
};

apiInstance.createWallet(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **createWalletRequest** | **CreateWalletRequest**|  |


### Return type

**CreateWalletResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Wallet created |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **creditWallet**
> void creditWallet()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiCreditWalletRequest = {
  // string
  id: "id_example",
  // CreditWalletRequest (optional)
  creditWalletRequest: {
    amount: {
      asset: "asset_example",
      amount: 1,
    },
    metadata: {
      "key": null,
    },
    reference: "reference_example",
    sources: [
      null,
    ],
    balance: "balance_example",
  },
};

apiInstance.creditWallet(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **creditWalletRequest** | **CreditWalletRequest**|  |
 **id** | [**string**] |  | defaults to undefined


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
**204** | Wallet successfully credited |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **debitWallet**
> DebitWalletResponse | void debitWallet()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiDebitWalletRequest = {
  // string
  id: "id_example",
  // DebitWalletRequest (optional)
  debitWalletRequest: {
    amount: {
      asset: "asset_example",
      amount: 1,
    },
    pending: true,
    metadata: {
      "key": null,
    },
    description: "description_example",
    destination: null,
    balances: [
      "balances_example",
    ],
  },
};

apiInstance.debitWallet(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **debitWalletRequest** | **DebitWalletRequest**|  |
 **id** | [**string**] |  | defaults to undefined


### Return type

**DebitWalletResponse | void**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Wallet successfully debited as a pending hold |  -  |
**204** | Wallet successfully debited |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **getBalance**
> GetBalanceResponse getBalance()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiGetBalanceRequest = {
  // string
  id: "id_example",
  // string
  balanceName: "balanceName_example",
};

apiInstance.getBalance(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | [**string**] |  | defaults to undefined
 **balanceName** | [**string**] |  | defaults to undefined


### Return type

**GetBalanceResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Balance summary |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **getHold**
> GetHoldResponse getHold()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiGetHoldRequest = {
  // string | The hold ID
  holdID: "holdID_example",
};

apiInstance.getHold(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **holdID** | [**string**] | The hold ID | defaults to undefined


### Return type

**GetHoldResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Holds |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **getHolds**
> GetHoldsResponse getHolds()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiGetHoldsRequest = {
  // number | The maximum number of results to return per page (optional)
  pageSize: 100,
  // string | The wallet to filter on (optional)
  walletID: "wallet1",
  // any | Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
  metadata: {},
  // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  (optional)
  cursor: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
};

apiInstance.getHolds(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pageSize** | [**number**] | The maximum number of results to return per page | (optional) defaults to 15
 **walletID** | [**string**] | The wallet to filter on | (optional) defaults to undefined
 **metadata** | **any** | Filter holds by metadata key value pairs. Nested objects can be used as seen in the example below. | (optional) defaults to undefined
 **cursor** | [**string**] | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  | (optional) defaults to undefined


### Return type

**GetHoldsResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Holds |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **getTransactions**
> GetTransactionsResponse getTransactions()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiGetTransactionsRequest = {
  // number | The maximum number of results to return per page (optional)
  pageSize: 100,
  // string | A wallet ID to filter on (optional)
  walletId: "wallet1",
  // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set.  (optional)
  cursor: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
};

apiInstance.getTransactions(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pageSize** | [**number**] | The maximum number of results to return per page | (optional) defaults to 15
 **walletId** | [**string**] | A wallet ID to filter on | (optional) defaults to undefined
 **cursor** | [**string**] | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the cursor is set.  | (optional) defaults to undefined


### Return type

**GetTransactionsResponse**

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

# **getWallet**
> GetWalletResponse getWallet()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiGetWalletRequest = {
  // string
  id: "id_example",
};

apiInstance.getWallet(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | [**string**] |  | defaults to undefined


### Return type

**GetWalletResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Wallet |  -  |
**404** | Wallet not found |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **listBalances**
> ListBalancesResponse listBalances()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiListBalancesRequest = {
  // string
  id: "id_example",
};

apiInstance.listBalances(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | [**string**] |  | defaults to undefined


### Return type

**ListBalancesResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Balances list |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **listWallets**
> ListWalletsResponse listWallets()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiListWalletsRequest = {
  // string | Filter on wallet name (optional)
  name: "wallet1",
  // any | Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
  metadata: {},
  // number | The maximum number of results to return per page (optional)
  pageSize: 100,
  // string | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  (optional)
  cursor: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
};

apiInstance.listWallets(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **name** | [**string**] | Filter on wallet name | (optional) defaults to undefined
 **metadata** | **any** | Filter wallets by metadata key value pairs. Nested objects can be used as seen in the example below. | (optional) defaults to undefined
 **pageSize** | [**number**] | The maximum number of results to return per page | (optional) defaults to 15
 **cursor** | [**string**] | Parameter used in pagination requests. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when the pagination token is set.  | (optional) defaults to undefined


### Return type

**ListWalletsResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **updateWallet**
> void updateWallet()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiUpdateWalletRequest = {
  // string
  id: "id_example",
  // UpdateWalletRequest (optional)
  updateWalletRequest: {
    metadata: {
      "key": null,
    },
  },
};

apiInstance.updateWallet(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **updateWalletRequest** | **UpdateWalletRequest**|  |
 **id** | [**string**] |  | defaults to undefined


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
**204** | Wallet successfully updated |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **voidHold**
> void voidHold()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:WalletsApiVoidHoldRequest = {
  // string
  holdId: "hold_id_example",
};

apiInstance.voidHold(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **holdId** | [**string**] |  | defaults to undefined


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
**204** | Hold successfully cancelled, funds returned to wallet |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **walletsgetServerInfo**
> ServerInfo walletsgetServerInfo()


### Example


```typescript
import { WalletsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WalletsApi(configuration);

let body:any = {};

apiInstance.walletsgetServerInfo(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters
This endpoint does not need any parameter.


### Return type

**ServerInfo**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Server information |  -  |
**0** | Error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

