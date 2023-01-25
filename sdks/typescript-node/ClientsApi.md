# formance.ClientsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**addScopeToClient**](ClientsApi.md#addScopeToClient) | **PUT** /api/auth/clients/{clientId}/scopes/{scopeId} | Add scope to client
[**createClient**](ClientsApi.md#createClient) | **POST** /api/auth/clients | Create client
[**createSecret**](ClientsApi.md#createSecret) | **POST** /api/auth/clients/{clientId}/secrets | Add a secret to a client
[**deleteClient**](ClientsApi.md#deleteClient) | **DELETE** /api/auth/clients/{clientId} | Delete client
[**deleteScopeFromClient**](ClientsApi.md#deleteScopeFromClient) | **DELETE** /api/auth/clients/{clientId}/scopes/{scopeId} | Delete scope from client
[**deleteSecret**](ClientsApi.md#deleteSecret) | **DELETE** /api/auth/clients/{clientId}/secrets/{secretId} | Delete a secret from a client
[**listClients**](ClientsApi.md#listClients) | **GET** /api/auth/clients | List clients
[**readClient**](ClientsApi.md#readClient) | **GET** /api/auth/clients/{clientId} | Read client
[**updateClient**](ClientsApi.md#updateClient) | **PUT** /api/auth/clients/{clientId} | Update client


# **addScopeToClient**
> void addScopeToClient()


### Example


```typescript
import { ClientsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ClientsApi(configuration);

let body:ClientsApiAddScopeToClientRequest = {
  // string | Client ID
  clientId: "clientId_example",
  // string | Scope ID
  scopeId: "scopeId_example",
};

apiInstance.addScopeToClient(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **clientId** | [**string**] | Client ID | defaults to undefined
 **scopeId** | [**string**] | Scope ID | defaults to undefined


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
**204** | Scope added to client |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **createClient**
> CreateClientResponse createClient()


### Example


```typescript
import { ClientsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ClientsApi(configuration);

let body:ClientsApiCreateClientRequest = {
  // ClientOptions (optional)
  body: {
    _public: true,
    redirectUris: [
      "redirectUris_example",
    ],
    description: "description_example",
    name: "name_example",
    trusted: true,
    postLogoutRedirectUris: [
      "postLogoutRedirectUris_example",
    ],
    metadata: {
      "key": null,
    },
  },
};

apiInstance.createClient(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **ClientOptions**|  |


### Return type

**CreateClientResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Client created |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **createSecret**
> CreateSecretResponse createSecret()


### Example


```typescript
import { ClientsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ClientsApi(configuration);

let body:ClientsApiCreateSecretRequest = {
  // string | Client ID
  clientId: "clientId_example",
  // SecretOptions (optional)
  body: {
    name: "name_example",
    metadata: {
      "key": null,
    },
  },
};

apiInstance.createSecret(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **SecretOptions**|  |
 **clientId** | [**string**] | Client ID | defaults to undefined


### Return type

**CreateSecretResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Created secret |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **deleteClient**
> void deleteClient()


### Example


```typescript
import { ClientsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ClientsApi(configuration);

let body:ClientsApiDeleteClientRequest = {
  // string | Client ID
  clientId: "clientId_example",
};

apiInstance.deleteClient(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **clientId** | [**string**] | Client ID | defaults to undefined


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
**204** | Client deleted |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **deleteScopeFromClient**
> void deleteScopeFromClient()


### Example


```typescript
import { ClientsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ClientsApi(configuration);

let body:ClientsApiDeleteScopeFromClientRequest = {
  // string | Client ID
  clientId: "clientId_example",
  // string | Scope ID
  scopeId: "scopeId_example",
};

apiInstance.deleteScopeFromClient(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **clientId** | [**string**] | Client ID | defaults to undefined
 **scopeId** | [**string**] | Scope ID | defaults to undefined


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
**204** | Scope deleted from client |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **deleteSecret**
> void deleteSecret()


### Example


```typescript
import { ClientsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ClientsApi(configuration);

let body:ClientsApiDeleteSecretRequest = {
  // string | Client ID
  clientId: "clientId_example",
  // string | Secret ID
  secretId: "secretId_example",
};

apiInstance.deleteSecret(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **clientId** | [**string**] | Client ID | defaults to undefined
 **secretId** | [**string**] | Secret ID | defaults to undefined


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
**204** | Secret deleted |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **listClients**
> ListClientsResponse listClients()


### Example


```typescript
import { ClientsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ClientsApi(configuration);

let body:any = {};

apiInstance.listClients(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters
This endpoint does not need any parameter.


### Return type

**ListClientsResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of clients |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **readClient**
> ReadClientResponse readClient()


### Example


```typescript
import { ClientsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ClientsApi(configuration);

let body:ClientsApiReadClientRequest = {
  // string | Client ID
  clientId: "clientId_example",
};

apiInstance.readClient(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **clientId** | [**string**] | Client ID | defaults to undefined


### Return type

**ReadClientResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Retrieved client |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **updateClient**
> CreateClientResponse updateClient()


### Example


```typescript
import { ClientsApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ClientsApi(configuration);

let body:ClientsApiUpdateClientRequest = {
  // string | Client ID
  clientId: "clientId_example",
  // ClientOptions (optional)
  body: {
    _public: true,
    redirectUris: [
      "redirectUris_example",
    ],
    description: "description_example",
    name: "name_example",
    trusted: true,
    postLogoutRedirectUris: [
      "postLogoutRedirectUris_example",
    ],
    metadata: {
      "key": null,
    },
  },
};

apiInstance.updateClient(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **ClientOptions**|  |
 **clientId** | [**string**] | Client ID | defaults to undefined


### Return type

**CreateClientResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Updated client |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

