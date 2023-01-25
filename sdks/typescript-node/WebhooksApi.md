# formance.WebhooksApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**activateConfig**](WebhooksApi.md#activateConfig) | **PUT** /api/webhooks/configs/{id}/activate | Activate one config
[**changeConfigSecret**](WebhooksApi.md#changeConfigSecret) | **PUT** /api/webhooks/configs/{id}/secret/change | Change the signing secret of a config
[**deactivateConfig**](WebhooksApi.md#deactivateConfig) | **PUT** /api/webhooks/configs/{id}/deactivate | Deactivate one config
[**deleteConfig**](WebhooksApi.md#deleteConfig) | **DELETE** /api/webhooks/configs/{id} | Delete one config
[**getManyConfigs**](WebhooksApi.md#getManyConfigs) | **GET** /api/webhooks/configs | Get many configs
[**insertConfig**](WebhooksApi.md#insertConfig) | **POST** /api/webhooks/configs | Insert a new config
[**testConfig**](WebhooksApi.md#testConfig) | **GET** /api/webhooks/configs/{id}/test | Test one config


# **activateConfig**
> ConfigResponse activateConfig()

Activate a webhooks config by ID, to start receiving webhooks to its endpoint.

### Example


```typescript
import { WebhooksApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WebhooksApi(configuration);

let body:WebhooksApiActivateConfigRequest = {
  // string | Config ID
  id: "4997257d-dfb6-445b-929c-cbe2ab182818",
};

apiInstance.activateConfig(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | [**string**] | Config ID | defaults to undefined


### Return type

**ConfigResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Config successfully activated. |  -  |
**304** | Config not modified, was already activated. |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **changeConfigSecret**
> ConfigResponse changeConfigSecret()

Change the signing secret of the endpoint of a webhooks config.  If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding) 

### Example


```typescript
import { WebhooksApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WebhooksApi(configuration);

let body:WebhooksApiChangeConfigSecretRequest = {
  // string | Config ID
  id: "4997257d-dfb6-445b-929c-cbe2ab182818",
  // ConfigChangeSecret (optional)
  configChangeSecret: {
    secret: "V0bivxRWveaoz08afqjU6Ko/jwO0Cb+3",
  },
};

apiInstance.changeConfigSecret(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **configChangeSecret** | **ConfigChangeSecret**|  |
 **id** | [**string**] | Config ID | defaults to undefined


### Return type

**ConfigResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Secret successfully changed. |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **deactivateConfig**
> ConfigResponse deactivateConfig()

Deactivate a webhooks config by ID, to stop receiving webhooks to its endpoint.

### Example


```typescript
import { WebhooksApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WebhooksApi(configuration);

let body:WebhooksApiDeactivateConfigRequest = {
  // string | Config ID
  id: "4997257d-dfb6-445b-929c-cbe2ab182818",
};

apiInstance.deactivateConfig(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | [**string**] | Config ID | defaults to undefined


### Return type

**ConfigResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Config successfully deactivated. |  -  |
**304** | Config not modified, was already deactivated. |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **deleteConfig**
> void deleteConfig()

Delete a webhooks config by ID.

### Example


```typescript
import { WebhooksApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WebhooksApi(configuration);

let body:WebhooksApiDeleteConfigRequest = {
  // string | Config ID
  id: "4997257d-dfb6-445b-929c-cbe2ab182818",
};

apiInstance.deleteConfig(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | [**string**] | Config ID | defaults to undefined


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
**200** | Config successfully deleted. |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **getManyConfigs**
> ConfigsResponse getManyConfigs()

Sorted by updated date descending

### Example


```typescript
import { WebhooksApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WebhooksApi(configuration);

let body:WebhooksApiGetManyConfigsRequest = {
  // string | Optional filter by Config ID (optional)
  id: "4997257d-dfb6-445b-929c-cbe2ab182818",
  // string | Optional filter by endpoint URL (optional)
  endpoint: "https://example.com",
};

apiInstance.getManyConfigs(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | [**string**] | Optional filter by Config ID | (optional) defaults to undefined
 **endpoint** | [**string**] | Optional filter by endpoint URL | (optional) defaults to undefined


### Return type

**ConfigsResponse**

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

# **insertConfig**
> ConfigResponse insertConfig(configUser)

Insert a new webhooks config.  The endpoint should be a valid https URL and be unique.  The secret is the endpoint's verification secret. If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding)  All eventTypes are converted to lower-case when inserted. 

### Example


```typescript
import { WebhooksApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WebhooksApi(configuration);

let body:WebhooksApiInsertConfigRequest = {
  // ConfigUser
  configUser: {
    endpoint: "https://example.com",
    secret: "V0bivxRWveaoz08afqjU6Ko/jwO0Cb+3",
    eventTypes: ["TYPE1","TYPE2"],
  },
};

apiInstance.insertConfig(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **configUser** | **ConfigUser**|  |


### Return type

**ConfigResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json, text/plain


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Config created successfully. |  -  |
**400** | Bad Request |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **testConfig**
> AttemptResponse testConfig()

Test a config by sending a webhook to its endpoint.

### Example


```typescript
import { WebhooksApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new WebhooksApi(configuration);

let body:WebhooksApiTestConfigRequest = {
  // string | Config ID
  id: "4997257d-dfb6-445b-929c-cbe2ab182818",
};

apiInstance.testConfig(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | [**string**] | Config ID | defaults to undefined


### Return type

**AttemptResponse**

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

