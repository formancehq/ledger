# OrchestrationApi

All URIs are relative to *http://localhost*

| Method | HTTP request | Description |
|------------- | ------------- | -------------|
| [**createWorkflow**](OrchestrationApi.md#createWorkflow) | **POST** api/orchestration/flows | Create workflow |
| [**getFlow**](OrchestrationApi.md#getFlow) | **GET** api/orchestration/flows/{flowId} | Get a flow by id |
| [**getWorkflowOccurrence**](OrchestrationApi.md#getWorkflowOccurrence) | **GET** api/orchestration/flows/{flowId}/runs/{runId} | Get a workflow occurrence by id |
| [**listFlows**](OrchestrationApi.md#listFlows) | **GET** api/orchestration/flows | List registered flows |
| [**listRuns**](OrchestrationApi.md#listRuns) | **GET** api/orchestration/flows/{flowId}/runs | List occurrences of a workflow |
| [**orchestrationgetServerInfo**](OrchestrationApi.md#orchestrationgetServerInfo) | **GET** api/orchestration/_info | Get server info |
| [**runWorkflow**](OrchestrationApi.md#runWorkflow) | **POST** api/orchestration/flows/{flowId}/runs | Run workflow |



## createWorkflow

> CreateWorkflowResponse createWorkflow(body)

Create workflow

Create a workflow

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.OrchestrationApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        OrchestrationApi apiInstance = new OrchestrationApi(defaultClient);
        WorkflowConfig body = new WorkflowConfig(); // WorkflowConfig | 
        try {
            CreateWorkflowResponse result = apiInstance.createWorkflow(body);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling OrchestrationApi#createWorkflow");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **body** | **WorkflowConfig**|  | [optional] |

### Return type

[**CreateWorkflowResponse**](CreateWorkflowResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | Created workflow |  -  |
| **0** | General error |  -  |


## getFlow

> GetWorkflowResponse getFlow(flowId)

Get a flow by id

Get a flow by id

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.OrchestrationApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        OrchestrationApi apiInstance = new OrchestrationApi(defaultClient);
        String flowId = "xxx"; // String | The flow id
        try {
            GetWorkflowResponse result = apiInstance.getFlow(flowId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling OrchestrationApi#getFlow");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **flowId** | **String**| The flow id | |

### Return type

[**GetWorkflowResponse**](GetWorkflowResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The workflow |  -  |
| **0** | General error |  -  |


## getWorkflowOccurrence

> GetWorkflowOccurrenceResponse getWorkflowOccurrence(flowId, runId)

Get a workflow occurrence by id

Get a workflow occurrence by id

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.OrchestrationApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        OrchestrationApi apiInstance = new OrchestrationApi(defaultClient);
        String flowId = "xxx"; // String | The flow id
        String runId = "xxx"; // String | The occurrence id
        try {
            GetWorkflowOccurrenceResponse result = apiInstance.getWorkflowOccurrence(flowId, runId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling OrchestrationApi#getWorkflowOccurrence");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **flowId** | **String**| The flow id | |
| **runId** | **String**| The occurrence id | |

### Return type

[**GetWorkflowOccurrenceResponse**](GetWorkflowOccurrenceResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | The workflow occurrence |  -  |
| **0** | General error |  -  |


## listFlows

> ListWorkflowsResponse listFlows()

List registered flows

List registered flows

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.OrchestrationApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        OrchestrationApi apiInstance = new OrchestrationApi(defaultClient);
        try {
            ListWorkflowsResponse result = apiInstance.listFlows();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling OrchestrationApi#listFlows");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**ListWorkflowsResponse**](ListWorkflowsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | List of workflows |  -  |
| **0** | General error |  -  |


## listRuns

> ListRunsResponse listRuns(flowId)

List occurrences of a workflow

List occurrences of a workflow

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.OrchestrationApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        OrchestrationApi apiInstance = new OrchestrationApi(defaultClient);
        String flowId = "xxx"; // String | The flow id
        try {
            ListRunsResponse result = apiInstance.listRuns(flowId);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling OrchestrationApi#listRuns");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **flowId** | **String**| The flow id | |

### Return type

[**ListRunsResponse**](ListRunsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | List of workflow occurrences |  -  |
| **0** | General error |  -  |


## orchestrationgetServerInfo

> ServerInfo orchestrationgetServerInfo()

Get server info

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.OrchestrationApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        OrchestrationApi apiInstance = new OrchestrationApi(defaultClient);
        try {
            ServerInfo result = apiInstance.orchestrationgetServerInfo();
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling OrchestrationApi#orchestrationgetServerInfo");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters

This endpoint does not need any parameter.

### Return type

[**ServerInfo**](ServerInfo.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | Server information |  -  |
| **0** | General error |  -  |


## runWorkflow

> RunWorkflowResponse runWorkflow(flowId, wait, requestBody)

Run workflow

Run workflow

### Example

```java
// Import classes:
import com.formance.formance.ApiClient;
import com.formance.formance.ApiException;
import com.formance.formance.Configuration;
import com.formance.formance.auth.*;
import com.formance.formance.models.*;
import com.formance.formance.api.OrchestrationApi;

public class Example {
    public static void main(String[] args) {
        ApiClient defaultClient = Configuration.getDefaultApiClient();
        defaultClient.setBasePath("http://localhost");
        
        // Configure OAuth2 access token for authorization: Authorization
        OAuth Authorization = (OAuth) defaultClient.getAuthentication("Authorization");
        Authorization.setAccessToken("YOUR ACCESS TOKEN");

        OrchestrationApi apiInstance = new OrchestrationApi(defaultClient);
        String flowId = "xxx"; // String | The flow id
        Boolean wait = true; // Boolean | Wait end of the workflow before return
        Map<String, String> requestBody = new HashMap(); // Map<String, String> | 
        try {
            RunWorkflowResponse result = apiInstance.runWorkflow(flowId, wait, requestBody);
            System.out.println(result);
        } catch (ApiException e) {
            System.err.println("Exception when calling OrchestrationApi#runWorkflow");
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }
    }
}
```

### Parameters


| Name | Type | Description  | Notes |
|------------- | ------------- | ------------- | -------------|
| **flowId** | **String**| The flow id | |
| **wait** | **Boolean**| Wait end of the workflow before return | [optional] |
| **requestBody** | [**Map&lt;String, String&gt;**](String.md)|  | [optional] |

### Return type

[**RunWorkflowResponse**](RunWorkflowResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **201** | The workflow occurrence |  -  |
| **0** | General error |  -  |

