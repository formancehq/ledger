# \MappingApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GetMapping**](MappingApi.md#GetMapping) | **Get** /api/ledger/{ledger}/mapping | Get the mapping of a ledger
[**UpdateMapping**](MappingApi.md#UpdateMapping) | **Put** /api/ledger/{ledger}/mapping | Update the mapping of a ledger



## GetMapping

> MappingResponse GetMapping(ctx, ledger).Execute()

Get the mapping of a ledger

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
    ledger := "ledger001" // string | Name of the ledger.

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.MappingApi.GetMapping(context.Background(), ledger).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `MappingApi.GetMapping``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetMapping`: MappingResponse
    fmt.Fprintf(os.Stdout, "Response from `MappingApi.GetMapping`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetMappingRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**MappingResponse**](MappingResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## UpdateMapping

> MappingResponse UpdateMapping(ctx, ledger).Mapping(mapping).Execute()

Update the mapping of a ledger

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
    ledger := "ledger001" // string | Name of the ledger.
    mapping := *client.NewMapping([]client.Contract{*client.NewContract(map[string]interface{}(123))}) // Mapping | 

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.MappingApi.UpdateMapping(context.Background(), ledger).Mapping(mapping).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `MappingApi.UpdateMapping``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `UpdateMapping`: MappingResponse
    fmt.Fprintf(os.Stdout, "Response from `MappingApi.UpdateMapping`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 

### Other Parameters

Other parameters are passed through a pointer to a apiUpdateMappingRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **mapping** | [**Mapping**](Mapping.md) |  | 

### Return type

[**MappingResponse**](MappingResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

