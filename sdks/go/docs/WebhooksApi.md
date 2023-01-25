# \WebhooksApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ActivateConfig**](WebhooksApi.md#ActivateConfig) | **Put** /api/webhooks/configs/{id}/activate | Activate one config
[**ChangeConfigSecret**](WebhooksApi.md#ChangeConfigSecret) | **Put** /api/webhooks/configs/{id}/secret/change | Change the signing secret of a config
[**DeactivateConfig**](WebhooksApi.md#DeactivateConfig) | **Put** /api/webhooks/configs/{id}/deactivate | Deactivate one config
[**DeleteConfig**](WebhooksApi.md#DeleteConfig) | **Delete** /api/webhooks/configs/{id} | Delete one config
[**GetManyConfigs**](WebhooksApi.md#GetManyConfigs) | **Get** /api/webhooks/configs | Get many configs
[**InsertConfig**](WebhooksApi.md#InsertConfig) | **Post** /api/webhooks/configs | Insert a new config
[**TestConfig**](WebhooksApi.md#TestConfig) | **Get** /api/webhooks/configs/{id}/test | Test one config



## ActivateConfig

> ConfigResponse ActivateConfig(ctx, id).Execute()

Activate one config



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
    id := "4997257d-dfb6-445b-929c-cbe2ab182818" // string | Config ID

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WebhooksApi.ActivateConfig(context.Background(), id).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WebhooksApi.ActivateConfig``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ActivateConfig`: ConfigResponse
    fmt.Fprintf(os.Stdout, "Response from `WebhooksApi.ActivateConfig`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** | Config ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiActivateConfigRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**ConfigResponse**](ConfigResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ChangeConfigSecret

> ConfigResponse ChangeConfigSecret(ctx, id).ConfigChangeSecret(configChangeSecret).Execute()

Change the signing secret of a config



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
    id := "4997257d-dfb6-445b-929c-cbe2ab182818" // string | Config ID
    configChangeSecret := *client.NewConfigChangeSecret() // ConfigChangeSecret |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WebhooksApi.ChangeConfigSecret(context.Background(), id).ConfigChangeSecret(configChangeSecret).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WebhooksApi.ChangeConfigSecret``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ChangeConfigSecret`: ConfigResponse
    fmt.Fprintf(os.Stdout, "Response from `WebhooksApi.ChangeConfigSecret`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** | Config ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiChangeConfigSecretRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **configChangeSecret** | [**ConfigChangeSecret**](ConfigChangeSecret.md) |  | 

### Return type

[**ConfigResponse**](ConfigResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## DeactivateConfig

> ConfigResponse DeactivateConfig(ctx, id).Execute()

Deactivate one config



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
    id := "4997257d-dfb6-445b-929c-cbe2ab182818" // string | Config ID

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WebhooksApi.DeactivateConfig(context.Background(), id).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WebhooksApi.DeactivateConfig``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `DeactivateConfig`: ConfigResponse
    fmt.Fprintf(os.Stdout, "Response from `WebhooksApi.DeactivateConfig`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** | Config ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiDeactivateConfigRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**ConfigResponse**](ConfigResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## DeleteConfig

> DeleteConfig(ctx, id).Execute()

Delete one config



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
    id := "4997257d-dfb6-445b-929c-cbe2ab182818" // string | Config ID

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WebhooksApi.DeleteConfig(context.Background(), id).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WebhooksApi.DeleteConfig``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** | Config ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiDeleteConfigRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

 (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetManyConfigs

> ConfigsResponse GetManyConfigs(ctx).Id(id).Endpoint(endpoint).Execute()

Get many configs



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
    id := "4997257d-dfb6-445b-929c-cbe2ab182818" // string | Optional filter by Config ID (optional)
    endpoint := "https://example.com" // string | Optional filter by endpoint URL (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WebhooksApi.GetManyConfigs(context.Background()).Id(id).Endpoint(endpoint).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WebhooksApi.GetManyConfigs``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetManyConfigs`: ConfigsResponse
    fmt.Fprintf(os.Stdout, "Response from `WebhooksApi.GetManyConfigs`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiGetManyConfigsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **id** | **string** | Optional filter by Config ID | 
 **endpoint** | **string** | Optional filter by endpoint URL | 

### Return type

[**ConfigsResponse**](ConfigsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## InsertConfig

> ConfigResponse InsertConfig(ctx).ConfigUser(configUser).Execute()

Insert a new config



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
    configUser := *client.NewConfigUser("https://example.com", []string{"TYPE1"}) // ConfigUser | 

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WebhooksApi.InsertConfig(context.Background()).ConfigUser(configUser).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WebhooksApi.InsertConfig``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `InsertConfig`: ConfigResponse
    fmt.Fprintf(os.Stdout, "Response from `WebhooksApi.InsertConfig`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiInsertConfigRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **configUser** | [**ConfigUser**](ConfigUser.md) |  | 

### Return type

[**ConfigResponse**](ConfigResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json, text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## TestConfig

> AttemptResponse TestConfig(ctx, id).Execute()

Test one config



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
    id := "4997257d-dfb6-445b-929c-cbe2ab182818" // string | Config ID

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.WebhooksApi.TestConfig(context.Background(), id).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `WebhooksApi.TestConfig``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `TestConfig`: AttemptResponse
    fmt.Fprintf(os.Stdout, "Response from `WebhooksApi.TestConfig`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**id** | **string** | Config ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiTestConfigRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**AttemptResponse**](AttemptResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

