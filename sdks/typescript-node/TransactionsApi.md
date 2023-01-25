# formance.TransactionsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**addMetadataOnTransaction**](TransactionsApi.md#addMetadataOnTransaction) | **POST** /api/ledger/{ledger}/transactions/{txid}/metadata | Set the metadata of a transaction by its ID
[**countTransactions**](TransactionsApi.md#countTransactions) | **HEAD** /api/ledger/{ledger}/transactions | Count the transactions from a ledger
[**createTransaction**](TransactionsApi.md#createTransaction) | **POST** /api/ledger/{ledger}/transactions | Create a new transaction to a ledger
[**createTransactions**](TransactionsApi.md#createTransactions) | **POST** /api/ledger/{ledger}/transactions/batch | Create a new batch of transactions to a ledger
[**getTransaction**](TransactionsApi.md#getTransaction) | **GET** /api/ledger/{ledger}/transactions/{txid} | Get transaction from a ledger by its ID
[**listTransactions**](TransactionsApi.md#listTransactions) | **GET** /api/ledger/{ledger}/transactions | List transactions from a ledger
[**revertTransaction**](TransactionsApi.md#revertTransaction) | **POST** /api/ledger/{ledger}/transactions/{txid}/revert | Revert a ledger transaction by its ID


# **addMetadataOnTransaction**
> void addMetadataOnTransaction()


### Example


```typescript
import { TransactionsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new TransactionsApi(configuration);

let body:TransactionsApiAddMetadataOnTransactionRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // number | Transaction ID.
  txid: 1234,
  // { [key: string]: any; } | metadata (optional)
  requestBody: {
    "key": null,
  },
};

apiInstance.addMetadataOnTransaction(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **requestBody** | **{ [key: string]: any; }**| metadata |
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **txid** | [**number**] | Transaction ID. | defaults to undefined


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

# **countTransactions**
> void countTransactions()


### Example


```typescript
import { TransactionsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new TransactionsApi(configuration);

let body:TransactionsApiCountTransactionsRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // string | Filter transactions by reference field. (optional)
  reference: "ref:001",
  // string | Filter transactions with postings involving given account, either as source or destination (regular expression placed between ^ and $). (optional)
  account: "users:001",
  // string | Filter transactions with postings involving given account at source (regular expression placed between ^ and $). (optional)
  source: "users:001",
  // string | Filter transactions with postings involving given account at destination (regular expression placed between ^ and $). (optional)
  destination: "users:001",
  // Date | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \"2023-01-02T15:04:01Z\" includes the first second of 4th minute).  (optional)
  startTime: new Date('1970-01-01T00:00:00.00Z'),
  // Date | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \"2023-01-02T15:04:01Z\" includes the first second of 4th minute). Deprecated, please use `startTime` instead.  (optional)
  startTime2: new Date('1970-01-01T00:00:00.00Z'),
  // Date | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \"2023-01-02T15:04:01Z\" excludes the first second of 4th minute).  (optional)
  endTime: new Date('1970-01-01T00:00:00.00Z'),
  // Date | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \"2023-01-02T15:04:01Z\" excludes the first second of 4th minute). Deprecated, please use `endTime` instead.  (optional)
  endTime2: new Date('1970-01-01T00:00:00.00Z'),
  // any | Filter transactions by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
  metadata: {},
};

apiInstance.countTransactions(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **reference** | [**string**] | Filter transactions by reference field. | (optional) defaults to undefined
 **account** | [**string**] | Filter transactions with postings involving given account, either as source or destination (regular expression placed between ^ and $). | (optional) defaults to undefined
 **source** | [**string**] | Filter transactions with postings involving given account at source (regular expression placed between ^ and $). | (optional) defaults to undefined
 **destination** | [**string**] | Filter transactions with postings involving given account at destination (regular expression placed between ^ and $). | (optional) defaults to undefined
 **startTime** | [**Date**] | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute).  | (optional) defaults to undefined
 **startTime2** | [**Date**] | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead.  | (optional) defaults to undefined
 **endTime** | [**Date**] | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute).  | (optional) defaults to undefined
 **endTime2** | [**Date**] | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead.  | (optional) defaults to undefined
 **metadata** | **any** | Filter transactions by metadata key value pairs. Nested objects can be used as seen in the example below. | (optional) defaults to undefined


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

# **createTransaction**
> TransactionsResponse createTransaction(postTransaction)


### Example


```typescript
import { TransactionsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new TransactionsApi(configuration);

let body:TransactionsApiCreateTransactionRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // PostTransaction | The request body must contain at least one of the following objects:   - `postings`: suitable for simple transactions   - `script`: enabling more complex transactions with Numscript 
  postTransaction: {
    timestamp: new Date('1970-01-01T00:00:00.00Z'),
    postings: [
      {
        amount: 100,
        asset: "COIN",
        destination: "users:002",
        source: "users:001",
      },
    ],
    script: {
      plain: `vars {
account $user
}
send [COIN 10] (
	source = @world
	destination = $user
)
`,
      vars: {},
    },
    reference: "ref:001",
    metadata: {
      "key": null,
    },
  },
  // boolean | Set the preview mode. Preview mode doesn't add the logs to the database or publish a message to the message broker. (optional)
  preview: true,
};

apiInstance.createTransaction(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **postTransaction** | **PostTransaction**| The request body must contain at least one of the following objects:   - &#x60;postings&#x60;: suitable for simple transactions   - &#x60;script&#x60;: enabling more complex transactions with Numscript  |
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **preview** | [**boolean**] | Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker. | (optional) defaults to undefined


### Return type

**TransactionsResponse**

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

# **createTransactions**
> TransactionsResponse createTransactions(transactions)


### Example


```typescript
import { TransactionsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new TransactionsApi(configuration);

let body:TransactionsApiCreateTransactionsRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // Transactions
  transactions: {
    transactions: [
      {
        postings: [
          {
            amount: 100,
            asset: "COIN",
            destination: "users:002",
            source: "users:001",
          },
        ],
        reference: "ref:001",
        metadata: {
          "key": null,
        },
        timestamp: new Date('1970-01-01T00:00:00.00Z'),
      },
    ],
  },
};

apiInstance.createTransactions(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **transactions** | **Transactions**|  |
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined


### Return type

**TransactionsResponse**

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

# **getTransaction**
> TransactionResponse getTransaction()


### Example


```typescript
import { TransactionsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new TransactionsApi(configuration);

let body:TransactionsApiGetTransactionRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // number | Transaction ID.
  txid: 1234,
};

apiInstance.getTransaction(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **txid** | [**number**] | Transaction ID. | defaults to undefined


### Return type

**TransactionResponse**

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

# **listTransactions**
> TransactionsCursorResponse listTransactions()

List transactions from a ledger, sorted by txid in descending order.

### Example


```typescript
import { TransactionsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new TransactionsApi(configuration);

let body:TransactionsApiListTransactionsRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // number | The maximum number of results to return per page.  (optional)
  pageSize: 100,
  // number | The maximum number of results to return per page. Deprecated, please use `pageSize` instead.  (optional)
  pageSize2: 100,
  // string | Pagination cursor, will return transactions after given txid (in descending order). (optional)
  after: "1234",
  // string | Find transactions by reference field. (optional)
  reference: "ref:001",
  // string | Filter transactions with postings involving given account, either as source or destination (regular expression placed between ^ and $). (optional)
  account: "users:001",
  // string | Filter transactions with postings involving given account at source (regular expression placed between ^ and $). (optional)
  source: "users:001",
  // string | Filter transactions with postings involving given account at destination (regular expression placed between ^ and $). (optional)
  destination: "users:001",
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
  // any | Filter transactions by metadata key value pairs. Nested objects can be used as seen in the example below. (optional)
  metadata: {},
};

apiInstance.listTransactions(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **pageSize** | [**number**] | The maximum number of results to return per page.  | (optional) defaults to 15
 **pageSize2** | [**number**] | The maximum number of results to return per page. Deprecated, please use &#x60;pageSize&#x60; instead.  | (optional) defaults to 15
 **after** | [**string**] | Pagination cursor, will return transactions after given txid (in descending order). | (optional) defaults to undefined
 **reference** | [**string**] | Find transactions by reference field. | (optional) defaults to undefined
 **account** | [**string**] | Filter transactions with postings involving given account, either as source or destination (regular expression placed between ^ and $). | (optional) defaults to undefined
 **source** | [**string**] | Filter transactions with postings involving given account at source (regular expression placed between ^ and $). | (optional) defaults to undefined
 **destination** | [**string**] | Filter transactions with postings involving given account at destination (regular expression placed between ^ and $). | (optional) defaults to undefined
 **startTime** | [**Date**] | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute).  | (optional) defaults to undefined
 **startTime2** | [**Date**] | Filter transactions that occurred after this timestamp. The format is RFC3339 and is inclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; includes the first second of 4th minute). Deprecated, please use &#x60;startTime&#x60; instead.  | (optional) defaults to undefined
 **endTime** | [**Date**] | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute).  | (optional) defaults to undefined
 **endTime2** | [**Date**] | Filter transactions that occurred before this timestamp. The format is RFC3339 and is exclusive (for example, \&quot;2023-01-02T15:04:01Z\&quot; excludes the first second of 4th minute). Deprecated, please use &#x60;endTime&#x60; instead.  | (optional) defaults to undefined
 **cursor** | [**string**] | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | (optional) defaults to undefined
 **paginationToken** | [**string**] | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set. Deprecated, please use &#x60;cursor&#x60; instead.  | (optional) defaults to undefined
 **metadata** | **any** | Filter transactions by metadata key value pairs. Nested objects can be used as seen in the example below. | (optional) defaults to undefined


### Return type

**TransactionsCursorResponse**

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

# **revertTransaction**
> TransactionResponse revertTransaction()


### Example


```typescript
import { TransactionsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new TransactionsApi(configuration);

let body:TransactionsApiRevertTransactionRequest = {
  // string | Name of the ledger.
  ledger: "ledger001",
  // number | Transaction ID.
  txid: 1234,
};

apiInstance.revertTransaction(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **ledger** | [**string**] | Name of the ledger. | defaults to undefined
 **txid** | [**number**] | Transaction ID. | defaults to undefined


### Return type

**TransactionResponse**

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

