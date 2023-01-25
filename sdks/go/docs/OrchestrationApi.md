# \OrchestrationApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**CreateWorkflow**](OrchestrationApi.md#CreateWorkflow) | **Post** /api/orchestration/flows | Create workflow
[**GetFlow**](OrchestrationApi.md#GetFlow) | **Get** /api/orchestration/flows/{flowId} | Get a flow by id
[**GetWorkflowOccurrence**](OrchestrationApi.md#GetWorkflowOccurrence) | **Get** /api/orchestration/flows/{flowId}/runs/{runId} | Get a workflow occurrence by id
[**ListFlows**](OrchestrationApi.md#ListFlows) | **Get** /api/orchestration/flows | List registered flows
[**ListRuns**](OrchestrationApi.md#ListRuns) | **Get** /api/orchestration/flows/{flowId}/runs | List occurrences of a workflow
[**OrchestrationgetServerInfo**](OrchestrationApi.md#OrchestrationgetServerInfo) | **Get** /api/orchestration/_info | Get server info
[**RunWorkflow**](OrchestrationApi.md#RunWorkflow) | **Post** /api/orchestration/flows/{flowId}/runs | Run workflow



## CreateWorkflow

> CreateWorkflowResponse CreateWorkflow(ctx).Body(body).Execute()

Create workflow



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    body := WorkflowConfig(987) // WorkflowConfig |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.OrchestrationApi.CreateWorkflow(context.Background()).Body(body).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `OrchestrationApi.CreateWorkflow``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `CreateWorkflow`: CreateWorkflowResponse
    fmt.Fprintf(os.Stdout, "Response from `OrchestrationApi.CreateWorkflow`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiCreateWorkflowRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **WorkflowConfig** |  | 

### Return type

[**CreateWorkflowResponse**](CreateWorkflowResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetFlow

> GetWorkflowResponse GetFlow(ctx, flowId).Execute()

Get a flow by id



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    flowId := "xxx" // string | The flow id

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.OrchestrationApi.GetFlow(context.Background(), flowId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `OrchestrationApi.GetFlow``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetFlow`: GetWorkflowResponse
    fmt.Fprintf(os.Stdout, "Response from `OrchestrationApi.GetFlow`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**flowId** | **string** | The flow id | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetFlowRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**GetWorkflowResponse**](GetWorkflowResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetWorkflowOccurrence

> GetWorkflowOccurrenceResponse GetWorkflowOccurrence(ctx, flowId, runId).Execute()

Get a workflow occurrence by id



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    flowId := "xxx" // string | The flow id
    runId := "xxx" // string | The occurrence id

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.OrchestrationApi.GetWorkflowOccurrence(context.Background(), flowId, runId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `OrchestrationApi.GetWorkflowOccurrence``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetWorkflowOccurrence`: GetWorkflowOccurrenceResponse
    fmt.Fprintf(os.Stdout, "Response from `OrchestrationApi.GetWorkflowOccurrence`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**flowId** | **string** | The flow id | 
**runId** | **string** | The occurrence id | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetWorkflowOccurrenceRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------



### Return type

[**GetWorkflowOccurrenceResponse**](GetWorkflowOccurrenceResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListFlows

> ListWorkflowsResponse ListFlows(ctx).Execute()

List registered flows



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.OrchestrationApi.ListFlows(context.Background()).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `OrchestrationApi.ListFlows``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListFlows`: ListWorkflowsResponse
    fmt.Fprintf(os.Stdout, "Response from `OrchestrationApi.ListFlows`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiListFlowsRequest struct via the builder pattern


### Return type

[**ListWorkflowsResponse**](ListWorkflowsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListRuns

> ListRunsResponse ListRuns(ctx, flowId).Execute()

List occurrences of a workflow



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    flowId := "xxx" // string | The flow id

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.OrchestrationApi.ListRuns(context.Background(), flowId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `OrchestrationApi.ListRuns``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListRuns`: ListRunsResponse
    fmt.Fprintf(os.Stdout, "Response from `OrchestrationApi.ListRuns`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**flowId** | **string** | The flow id | 

### Other Parameters

Other parameters are passed through a pointer to a apiListRunsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**ListRunsResponse**](ListRunsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## OrchestrationgetServerInfo

> ServerInfo OrchestrationgetServerInfo(ctx).Execute()

Get server info

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.OrchestrationApi.OrchestrationgetServerInfo(context.Background()).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `OrchestrationApi.OrchestrationgetServerInfo``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `OrchestrationgetServerInfo`: ServerInfo
    fmt.Fprintf(os.Stdout, "Response from `OrchestrationApi.OrchestrationgetServerInfo`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiOrchestrationgetServerInfoRequest struct via the builder pattern


### Return type

[**ServerInfo**](ServerInfo.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## RunWorkflow

> RunWorkflowResponse RunWorkflow(ctx, flowId).Wait(wait).RequestBody(requestBody).Execute()

Run workflow



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "./openapi"
)

func main() {
    flowId := "xxx" // string | The flow id
    wait := true // bool | Wait end of the workflow before return (optional)
    requestBody := map[string]string{"key": "Inner_example"} // map[string]string |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.OrchestrationApi.RunWorkflow(context.Background(), flowId).Wait(wait).RequestBody(requestBody).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `OrchestrationApi.RunWorkflow``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `RunWorkflow`: RunWorkflowResponse
    fmt.Fprintf(os.Stdout, "Response from `OrchestrationApi.RunWorkflow`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**flowId** | **string** | The flow id | 

### Other Parameters

Other parameters are passed through a pointer to a apiRunWorkflowRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **wait** | **bool** | Wait end of the workflow before return | 
 **requestBody** | **map[string]string** |  | 

### Return type

[**RunWorkflowResponse**](RunWorkflowResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

