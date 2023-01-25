# formance.PaymentsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**connectorsStripeTransfer**](PaymentsApi.md#connectorsStripeTransfer) | **POST** /api/payments/connectors/stripe/transfer | Transfer funds between Stripe accounts
[**getConnectorTask**](PaymentsApi.md#getConnectorTask) | **GET** /api/payments/connectors/{connector}/tasks/{taskId} | Read a specific task of the connector
[**getPayment**](PaymentsApi.md#getPayment) | **GET** /api/payments/payments/{paymentId} | Get a payment
[**installConnector**](PaymentsApi.md#installConnector) | **POST** /api/payments/connectors/{connector} | Install a connector
[**listAllConnectors**](PaymentsApi.md#listAllConnectors) | **GET** /api/payments/connectors | List all installed connectors
[**listConfigsAvailableConnectors**](PaymentsApi.md#listConfigsAvailableConnectors) | **GET** /api/payments/connectors/configs | List the configs of each available connector
[**listConnectorTasks**](PaymentsApi.md#listConnectorTasks) | **GET** /api/payments/connectors/{connector}/tasks | List tasks from a connector
[**listPayments**](PaymentsApi.md#listPayments) | **GET** /api/payments/payments | List payments
[**paymentslistAccounts**](PaymentsApi.md#paymentslistAccounts) | **GET** /api/payments/accounts | List accounts
[**readConnectorConfig**](PaymentsApi.md#readConnectorConfig) | **GET** /api/payments/connectors/{connector}/config | Read the config of a connector
[**resetConnector**](PaymentsApi.md#resetConnector) | **POST** /api/payments/connectors/{connector}/reset | Reset a connector
[**uninstallConnector**](PaymentsApi.md#uninstallConnector) | **DELETE** /api/payments/connectors/{connector} | Uninstall a connector


# **connectorsStripeTransfer**
> any connectorsStripeTransfer(stripeTransferRequest)

Execute a transfer between two Stripe accounts.

### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:PaymentsApiConnectorsStripeTransferRequest = {
  // StripeTransferRequest
  stripeTransferRequest: {
    amount: 100,
    asset: "USD",
    destination: "acct_1Gqj58KZcSIg2N2q",
    metadata: {},
  },
};

apiInstance.connectorsStripeTransfer(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **stripeTransferRequest** | **StripeTransferRequest**|  |


### Return type

**any**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | OK |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **getConnectorTask**
> TaskResponse getConnectorTask()

Get a specific task associated to the connector.

### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:PaymentsApiGetConnectorTaskRequest = {
  // Connector | The name of the connector.
  connector: "STRIPE",
  // string | The task ID.
  taskId: "task1",
};

apiInstance.getConnectorTask(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **connector** | **Connector** | The name of the connector. | defaults to undefined
 **taskId** | [**string**] | The task ID. | defaults to undefined


### Return type

**TaskResponse**

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

# **getPayment**
> PaymentResponse getPayment()


### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:PaymentsApiGetPaymentRequest = {
  // string | The payment ID.
  paymentId: "XXX",
};

apiInstance.getPayment(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **paymentId** | [**string**] | The payment ID. | defaults to undefined


### Return type

**PaymentResponse**

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

# **installConnector**
> void installConnector(connectorConfig)

Install a connector by its name and config.

### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:PaymentsApiInstallConnectorRequest = {
  // Connector | The name of the connector.
  connector: "STRIPE",
  // ConnectorConfig
  connectorConfig: null,
};

apiInstance.installConnector(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **connectorConfig** | **ConnectorConfig**|  |
 **connector** | **Connector** | The name of the connector. | defaults to undefined


### Return type

**void**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | No content |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **listAllConnectors**
> ConnectorsResponse listAllConnectors()

List all installed connectors.

### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:any = {};

apiInstance.listAllConnectors(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters
This endpoint does not need any parameter.


### Return type

**ConnectorsResponse**

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

# **listConfigsAvailableConnectors**
> ConnectorsConfigsResponse listConfigsAvailableConnectors()

List the configs of each available connector.

### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:any = {};

apiInstance.listConfigsAvailableConnectors(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters
This endpoint does not need any parameter.


### Return type

**ConnectorsConfigsResponse**

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

# **listConnectorTasks**
> TasksCursor listConnectorTasks()

List all tasks associated with this connector.

### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:PaymentsApiListConnectorTasksRequest = {
  // Connector | The name of the connector.
  connector: "STRIPE",
  // number | The maximum number of results to return per page.  (optional)
  pageSize: 100,
  // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
  cursor: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
};

apiInstance.listConnectorTasks(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **connector** | **Connector** | The name of the connector. | defaults to undefined
 **pageSize** | [**number**] | The maximum number of results to return per page.  | (optional) defaults to 15
 **cursor** | [**string**] | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | (optional) defaults to undefined


### Return type

**TasksCursor**

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

# **listPayments**
> PaymentsCursor listPayments()


### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:PaymentsApiListPaymentsRequest = {
  // number | The maximum number of results to return per page.  (optional)
  pageSize: 100,
  // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
  cursor: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
  // Array<string> | Fields used to sort payments (default is date:desc). (optional)
  sort: [
    "date:asc,status:desc",
  ],
};

apiInstance.listPayments(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pageSize** | [**number**] | The maximum number of results to return per page.  | (optional) defaults to 15
 **cursor** | [**string**] | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | (optional) defaults to undefined
 **sort** | **Array&lt;string&gt;** | Fields used to sort payments (default is date:desc). | (optional) defaults to undefined


### Return type

**PaymentsCursor**

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

# **paymentslistAccounts**
> AccountsCursor paymentslistAccounts()


### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:PaymentsApiPaymentslistAccountsRequest = {
  // number | The maximum number of results to return per page.  (optional)
  pageSize: 100,
  // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
  cursor: "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
  // Array<string> | Fields used to sort payments (default is date:desc). (optional)
  sort: [
    "date:asc,status:desc",
  ],
};

apiInstance.paymentslistAccounts(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pageSize** | [**number**] | The maximum number of results to return per page.  | (optional) defaults to 15
 **cursor** | [**string**] | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | (optional) defaults to undefined
 **sort** | **Array&lt;string&gt;** | Fields used to sort payments (default is date:desc). | (optional) defaults to undefined


### Return type

**AccountsCursor**

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

# **readConnectorConfig**
> ConnectorConfigResponse readConnectorConfig()

Read connector config

### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:PaymentsApiReadConnectorConfigRequest = {
  // Connector | The name of the connector.
  connector: "STRIPE",
};

apiInstance.readConnectorConfig(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **connector** | **Connector** | The name of the connector. | defaults to undefined


### Return type

**ConnectorConfigResponse**

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

# **resetConnector**
> void resetConnector()

Reset a connector by its name. It will remove the connector and ALL PAYMENTS generated with it. 

### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:PaymentsApiResetConnectorRequest = {
  // Connector | The name of the connector.
  connector: "STRIPE",
};

apiInstance.resetConnector(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **connector** | **Connector** | The name of the connector. | defaults to undefined


### Return type

**void**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | No content |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **uninstallConnector**
> void uninstallConnector()

Uninstall a connector by its name.

### Example


```typescript
import { PaymentsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new PaymentsApi(configuration);

let body:PaymentsApiUninstallConnectorRequest = {
  // Connector | The name of the connector.
  connector: "STRIPE",
};

apiInstance.uninstallConnector(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **connector** | **Connector** | The name of the connector. | defaults to undefined


### Return type

**void**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: Not defined


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**204** | No content |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

