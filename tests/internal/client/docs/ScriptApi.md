# \ScriptApi

All URIs are relative to *https://.o.numary.cloud/ledger*

Method | HTTP request | Description
------------- | ------------- | -------------
[**RunScript**](ScriptApi.md#RunScript) | **Post** /{ledger}/script | Execute a Numscript.



## RunScript

> ScriptResult RunScript(ctx, ledger).Script(script).IdempotencyKey(idempotencyKey).Preview(preview).Execute()

Execute a Numscript.

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
    ledger := "ledger001" // string | Name of the ledger.
    script := *client.NewScript("vars {
account $user
}
send [COIN 10] (
	source = @world
	destination = $user
)
") // Script | 
    idempotencyKey := "idempotencyKey_example" // string |  (optional)
    preview := true // bool | Set the preview mode. Preview mode doesn't add the logs to the database or publish a message to the message broker. (optional)

    configuration := client.NewConfiguration()
    api_client := client.NewAPIClient(configuration)
    resp, r, err := api_client.ScriptApi.RunScript(context.Background(), ledger).Script(script).IdempotencyKey(idempotencyKey).Preview(preview).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `ScriptApi.RunScript``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `RunScript`: ScriptResult
    fmt.Fprintf(os.Stdout, "Response from `ScriptApi.RunScript`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**ledger** | **string** | Name of the ledger. | 

### Other Parameters

Other parameters are passed through a pointer to a apiRunScriptRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **script** | [**Script**](Script.md) |  | 
 **idempotencyKey** | **string** |  | 
 **preview** | **bool** | Set the preview mode. Preview mode doesn&#39;t add the logs to the database or publish a message to the message broker. | 

### Return type

[**ScriptResult**](ScriptResult.md)

### Authorization

[basicAuth](../README.md#basicAuth)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

