# \ClientsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**AddScopeToClient**](ClientsApi.md#AddScopeToClient) | **Put** /api/auth/clients/{clientId}/scopes/{scopeId} | Add scope to client
[**CreateClient**](ClientsApi.md#CreateClient) | **Post** /api/auth/clients | Create client
[**CreateSecret**](ClientsApi.md#CreateSecret) | **Post** /api/auth/clients/{clientId}/secrets | Add a secret to a client
[**DeleteClient**](ClientsApi.md#DeleteClient) | **Delete** /api/auth/clients/{clientId} | Delete client
[**DeleteScopeFromClient**](ClientsApi.md#DeleteScopeFromClient) | **Delete** /api/auth/clients/{clientId}/scopes/{scopeId} | Delete scope from client
[**DeleteSecret**](ClientsApi.md#DeleteSecret) | **Delete** /api/auth/clients/{clientId}/secrets/{secretId} | Delete a secret from a client
[**ListClients**](ClientsApi.md#ListClients) | **Get** /api/auth/clients | List clients
[**ReadClient**](ClientsApi.md#ReadClient) | **Get** /api/auth/clients/{clientId} | Read client
[**UpdateClient**](ClientsApi.md#UpdateClient) | **Put** /api/auth/clients/{clientId} | Update client



## AddScopeToClient

> AddScopeToClient(ctx, clientId, scopeId).Execute()

Add scope to client

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
    clientId := "clientId_example" // string | Client ID
    scopeId := "scopeId_example" // string | Scope ID

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.ClientsApi.AddScopeToClient(context.Background(), clientId, scopeId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ClientsApi.AddScopeToClient``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**clientId** | **string** | Client ID | 
**scopeId** | **string** | Scope ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiAddScopeToClientRequest struct via the builder pattern


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


## CreateClient

> CreateClientResponse CreateClient(ctx).Body(body).Execute()

Create client

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
    body := ClientOptions(987) // ClientOptions |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.ClientsApi.CreateClient(context.Background()).Body(body).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ClientsApi.CreateClient``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `CreateClient`: CreateClientResponse
    fmt.Fprintf(os.Stdout, "Response from `ClientsApi.CreateClient`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiCreateClientRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **body** | **ClientOptions** |  | 

### Return type

[**CreateClientResponse**](CreateClientResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## CreateSecret

> CreateSecretResponse CreateSecret(ctx, clientId).Body(body).Execute()

Add a secret to a client

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
    clientId := "clientId_example" // string | Client ID
    body := SecretOptions(987) // SecretOptions |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.ClientsApi.CreateSecret(context.Background(), clientId).Body(body).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ClientsApi.CreateSecret``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `CreateSecret`: CreateSecretResponse
    fmt.Fprintf(os.Stdout, "Response from `ClientsApi.CreateSecret`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**clientId** | **string** | Client ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiCreateSecretRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **body** | **SecretOptions** |  | 

### Return type

[**CreateSecretResponse**](CreateSecretResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## DeleteClient

> DeleteClient(ctx, clientId).Execute()

Delete client

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
    clientId := "clientId_example" // string | Client ID

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.ClientsApi.DeleteClient(context.Background(), clientId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ClientsApi.DeleteClient``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**clientId** | **string** | Client ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiDeleteClientRequest struct via the builder pattern


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


## DeleteScopeFromClient

> DeleteScopeFromClient(ctx, clientId, scopeId).Execute()

Delete scope from client

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
    clientId := "clientId_example" // string | Client ID
    scopeId := "scopeId_example" // string | Scope ID

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.ClientsApi.DeleteScopeFromClient(context.Background(), clientId, scopeId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ClientsApi.DeleteScopeFromClient``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**clientId** | **string** | Client ID | 
**scopeId** | **string** | Scope ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiDeleteScopeFromClientRequest struct via the builder pattern


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


## DeleteSecret

> DeleteSecret(ctx, clientId, secretId).Execute()

Delete a secret from a client

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
    clientId := "clientId_example" // string | Client ID
    secretId := "secretId_example" // string | Secret ID

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.ClientsApi.DeleteSecret(context.Background(), clientId, secretId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ClientsApi.DeleteSecret``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**clientId** | **string** | Client ID | 
**secretId** | **string** | Secret ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiDeleteSecretRequest struct via the builder pattern


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


## ListClients

> ListClientsResponse ListClients(ctx).Execute()

List clients

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
    resp, r, err := apiClient.ClientsApi.ListClients(context.Background()).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ClientsApi.ListClients``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListClients`: ListClientsResponse
    fmt.Fprintf(os.Stdout, "Response from `ClientsApi.ListClients`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiListClientsRequest struct via the builder pattern


### Return type

[**ListClientsResponse**](ListClientsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ReadClient

> ReadClientResponse ReadClient(ctx, clientId).Execute()

Read client

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
    clientId := "clientId_example" // string | Client ID

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.ClientsApi.ReadClient(context.Background(), clientId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ClientsApi.ReadClient``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ReadClient`: ReadClientResponse
    fmt.Fprintf(os.Stdout, "Response from `ClientsApi.ReadClient`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**clientId** | **string** | Client ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiReadClientRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**ReadClientResponse**](ReadClientResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## UpdateClient

> CreateClientResponse UpdateClient(ctx, clientId).Body(body).Execute()

Update client

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
    clientId := "clientId_example" // string | Client ID
    body := ClientOptions(987) // ClientOptions |  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.ClientsApi.UpdateClient(context.Background(), clientId).Body(body).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ClientsApi.UpdateClient``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `UpdateClient`: CreateClientResponse
    fmt.Fprintf(os.Stdout, "Response from `ClientsApi.UpdateClient`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**clientId** | **string** | Client ID | 

### Other Parameters

Other parameters are passed through a pointer to a apiUpdateClientRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **body** | **ClientOptions** |  | 

### Return type

[**CreateClientResponse**](CreateClientResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

