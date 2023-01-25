# formance.ScopesApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**addTransientScope**](ScopesApi.md#addTransientScope) | **PUT** /api/auth/scopes/{scopeId}/transient/{transientScopeId} | Add a transient scope to a scope
[**createScope**](ScopesApi.md#createScope) | **POST** /api/auth/scopes | Create scope
[**deleteScope**](ScopesApi.md#deleteScope) | **DELETE** /api/auth/scopes/{scopeId} | Delete scope
[**deleteTransientScope**](ScopesApi.md#deleteTransientScope) | **DELETE** /api/auth/scopes/{scopeId}/transient/{transientScopeId} | Delete a transient scope from a scope
[**listScopes**](ScopesApi.md#listScopes) | **GET** /api/auth/scopes | List scopes
[**readScope**](ScopesApi.md#readScope) | **GET** /api/auth/scopes/{scopeId} | Read scope
[**updateScope**](ScopesApi.md#updateScope) | **PUT** /api/auth/scopes/{scopeId} | Update scope


# **addTransientScope**
> void addTransientScope()

Add a transient scope to a scope

### Example


```typescript
import { ScopesApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ScopesApi(configuration);

let body:ScopesApiAddTransientScopeRequest = {
  // string | Scope ID
  scopeId: "scopeId_example",
  // string | Transient scope ID
  transientScopeId: "transientScopeId_example",
};

apiInstance.addTransientScope(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **scopeId** | [**string**] | Scope ID | defaults to undefined
 **transientScopeId** | [**string**] | Transient scope ID | defaults to undefined


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
**204** | Scope added |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **createScope**
> CreateScopeResponse createScope()

Create scope

### Example


```typescript
import { ScopesApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ScopesApi(configuration);

let body:ScopesApiCreateScopeRequest = {
  // ScopeOptions (optional)
  body: {
    label: "label_example",
    metadata: {
      "key": null,
    },
  },
};

apiInstance.createScope(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **ScopeOptions**|  |


### Return type

**CreateScopeResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Created scope |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **deleteScope**
> void deleteScope()

Delete scope

### Example


```typescript
import { ScopesApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ScopesApi(configuration);

let body:ScopesApiDeleteScopeRequest = {
  // string | Scope ID
  scopeId: "scopeId_example",
};

apiInstance.deleteScope(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
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
**204** | Scope deleted |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **deleteTransientScope**
> void deleteTransientScope()

Delete a transient scope from a scope

### Example


```typescript
import { ScopesApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ScopesApi(configuration);

let body:ScopesApiDeleteTransientScopeRequest = {
  // string | Scope ID
  scopeId: "scopeId_example",
  // string | Transient scope ID
  transientScopeId: "transientScopeId_example",
};

apiInstance.deleteTransientScope(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **scopeId** | [**string**] | Scope ID | defaults to undefined
 **transientScopeId** | [**string**] | Transient scope ID | defaults to undefined


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
**204** | Transient scope deleted |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **listScopes**
> ListScopesResponse listScopes()

List Scopes

### Example


```typescript
import { ScopesApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ScopesApi(configuration);

let body:any = {};

apiInstance.listScopes(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters
This endpoint does not need any parameter.


### Return type

**ListScopesResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of scopes |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **readScope**
> CreateScopeResponse readScope()

Read scope

### Example


```typescript
import { ScopesApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ScopesApi(configuration);

let body:ScopesApiReadScopeRequest = {
  // string | Scope ID
  scopeId: "scopeId_example",
};

apiInstance.readScope(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **scopeId** | [**string**] | Scope ID | defaults to undefined


### Return type

**CreateScopeResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Retrieved scope |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **updateScope**
> CreateScopeResponse updateScope()

Update scope

### Example


```typescript
import { ScopesApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new ScopesApi(configuration);

let body:ScopesApiUpdateScopeRequest = {
  // string | Scope ID
  scopeId: "scopeId_example",
  // ScopeOptions (optional)
  body: {
    label: "label_example",
    metadata: {
      "key": null,
    },
  },
};

apiInstance.updateScope(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **ScopeOptions**|  |
 **scopeId** | [**string**] | Scope ID | defaults to undefined


### Return type

**CreateScopeResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | Updated scope |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

