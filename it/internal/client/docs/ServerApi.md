# \ServerApi

All URIs are relative to *https://.o.numary.cloud/ledger*

Method | HTTP request | Description
------------- | ------------- | -------------
[**GetInfo**](ServerApi.md#GetInfo) | **Get** /_info | Show server information.



## GetInfo

> ConfigInfoResponse GetInfo(ctx).Execute()

Show server information.

### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    client "github.com/numary/numary-go"
)

func main() {

    configuration := client.NewConfiguration()
    api_client := client.NewAPIClient(configuration)
    resp, r, err := api_client.ServerApi.GetInfo(context.Background()).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ServerApi.GetInfo``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetInfo`: ConfigInfoResponse
    fmt.Fprintf(os.Stdout, "Response from `ServerApi.GetInfo`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiGetInfoRequest struct via the builder pattern


### Return type

[**ConfigInfoResponse**](ConfigInfoResponse.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

