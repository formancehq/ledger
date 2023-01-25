# formance.OrchestrationApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**createWorkflow**](OrchestrationApi.md#createWorkflow) | **POST** /api/orchestration/flows | Create workflow
[**getFlow**](OrchestrationApi.md#getFlow) | **GET** /api/orchestration/flows/{flowId} | Get a flow by id
[**getWorkflowOccurrence**](OrchestrationApi.md#getWorkflowOccurrence) | **GET** /api/orchestration/flows/{flowId}/runs/{runId} | Get a workflow occurrence by id
[**listFlows**](OrchestrationApi.md#listFlows) | **GET** /api/orchestration/flows | List registered flows
[**listRuns**](OrchestrationApi.md#listRuns) | **GET** /api/orchestration/flows/{flowId}/runs | List occurrences of a workflow
[**orchestrationgetServerInfo**](OrchestrationApi.md#orchestrationgetServerInfo) | **GET** /api/orchestration/_info | Get server info
[**runWorkflow**](OrchestrationApi.md#runWorkflow) | **POST** /api/orchestration/flows/{flowId}/runs | Run workflow


# **createWorkflow**
> CreateWorkflowResponse createWorkflow()

Create a workflow

### Example


```typescript
import { OrchestrationApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new OrchestrationApi(configuration);

let body:OrchestrationApiCreateWorkflowRequest = {
  // WorkflowConfig (optional)
  body: {
    stages: [
      {
        "key": null,
      },
    ],
  },
};

apiInstance.createWorkflow(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **WorkflowConfig**|  |


### Return type

**CreateWorkflowResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | Created workflow |  -  |
**0** | General error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **getFlow**
> GetWorkflowResponse getFlow()

Get a flow by id

### Example


```typescript
import { OrchestrationApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new OrchestrationApi(configuration);

let body:OrchestrationApiGetFlowRequest = {
  // string | The flow id
  flowId: "xxx",
};

apiInstance.getFlow(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **flowId** | [**string**] | The flow id | defaults to undefined


### Return type

**GetWorkflowResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The workflow |  -  |
**0** | General error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **getWorkflowOccurrence**
> GetWorkflowOccurrenceResponse getWorkflowOccurrence()

Get a workflow occurrence by id

### Example


```typescript
import { OrchestrationApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new OrchestrationApi(configuration);

let body:OrchestrationApiGetWorkflowOccurrenceRequest = {
  // string | The flow id
  flowId: "xxx",
  // string | The occurrence id
  runId: "xxx",
};

apiInstance.getWorkflowOccurrence(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **flowId** | [**string**] | The flow id | defaults to undefined
 **runId** | [**string**] | The occurrence id | defaults to undefined


### Return type

**GetWorkflowOccurrenceResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | The workflow occurrence |  -  |
**0** | General error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **listFlows**
> ListWorkflowsResponse listFlows()

List registered flows

### Example


```typescript
import { OrchestrationApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new OrchestrationApi(configuration);

let body:any = {};

apiInstance.listFlows(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters
This endpoint does not need any parameter.


### Return type

**ListWorkflowsResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of workflows |  -  |
**0** | General error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **listRuns**
> ListRunsResponse listRuns()

List occurrences of a workflow

### Example


```typescript
import { OrchestrationApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new OrchestrationApi(configuration);

let body:OrchestrationApiListRunsRequest = {
  // string | The flow id
  flowId: "xxx",
};

apiInstance.listRuns(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **flowId** | [**string**] | The flow id | defaults to undefined


### Return type

**ListRunsResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | List of workflow occurrences |  -  |
**0** | General error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **orchestrationgetServerInfo**
> ServerInfo orchestrationgetServerInfo()


### Example


```typescript
import { OrchestrationApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new OrchestrationApi(configuration);

let body:any = {};

apiInstance.orchestrationgetServerInfo(body).then((data:any) => {
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
**0** | General error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

# **runWorkflow**
> RunWorkflowResponse runWorkflow()

Run workflow

### Example


```typescript
import { OrchestrationApi, createConfiguration } from '@formancehq/formance';
import * as fs from 'fs';

const configuration = createConfiguration();
const apiInstance = new OrchestrationApi(configuration);

let body:OrchestrationApiRunWorkflowRequest = {
  // string | The flow id
  flowId: "xxx",
  // boolean | Wait end of the workflow before return (optional)
  wait: true,
  // { [key: string]: string; } (optional)
  requestBody: {
    "key": "key_example",
  },
};

apiInstance.runWorkflow(body).then((data:any) => {
  console.log('API called successfully. Returned data: ' + data);
}).catch((error:any) => console.error(error));
```


### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **requestBody** | **{ [key: string]: string; }**|  |
 **flowId** | [**string**] | The flow id | defaults to undefined
 **wait** | [**boolean**] | Wait end of the workflow before return | (optional) defaults to undefined


### Return type

**RunWorkflowResponse**

### Authorization

[Authorization](README.md#Authorization)

### HTTP request headers

 - **Content-Type**: application/json
 - **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**201** | The workflow occurrence |  -  |
**0** | General error |  -  |

[[Back to top]](#) [[Back to API list]](README.md#documentation-for-api-endpoints) [[Back to Model list]](README.md#documentation-for-models) [[Back to README]](README.md)

