# \PaymentsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**ConnectorsStripeTransfer**](PaymentsApi.md#ConnectorsStripeTransfer) | **Post** /api/payments/connectors/stripe/transfer | Transfer funds between Stripe accounts
[**GetConnectorTask**](PaymentsApi.md#GetConnectorTask) | **Get** /api/payments/connectors/{connector}/tasks/{taskId} | Read a specific task of the connector
[**GetPayment**](PaymentsApi.md#GetPayment) | **Get** /api/payments/payments/{paymentId} | Get a payment
[**InstallConnector**](PaymentsApi.md#InstallConnector) | **Post** /api/payments/connectors/{connector} | Install a connector
[**ListAllConnectors**](PaymentsApi.md#ListAllConnectors) | **Get** /api/payments/connectors | List all installed connectors
[**ListConfigsAvailableConnectors**](PaymentsApi.md#ListConfigsAvailableConnectors) | **Get** /api/payments/connectors/configs | List the configs of each available connector
[**ListConnectorTasks**](PaymentsApi.md#ListConnectorTasks) | **Get** /api/payments/connectors/{connector}/tasks | List tasks from a connector
[**ListPayments**](PaymentsApi.md#ListPayments) | **Get** /api/payments/payments | List payments
[**PaymentslistAccounts**](PaymentsApi.md#PaymentslistAccounts) | **Get** /api/payments/accounts | List accounts
[**ReadConnectorConfig**](PaymentsApi.md#ReadConnectorConfig) | **Get** /api/payments/connectors/{connector}/config | Read the config of a connector
[**ResetConnector**](PaymentsApi.md#ResetConnector) | **Post** /api/payments/connectors/{connector}/reset | Reset a connector
[**UninstallConnector**](PaymentsApi.md#UninstallConnector) | **Delete** /api/payments/connectors/{connector} | Uninstall a connector



## ConnectorsStripeTransfer

> map[string]interface{} ConnectorsStripeTransfer(ctx).StripeTransferRequest(stripeTransferRequest).Execute()

Transfer funds between Stripe accounts



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
    stripeTransferRequest := *client.NewStripeTransferRequest() // StripeTransferRequest | 

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.PaymentsApi.ConnectorsStripeTransfer(context.Background()).StripeTransferRequest(stripeTransferRequest).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.ConnectorsStripeTransfer``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ConnectorsStripeTransfer`: map[string]interface{}
    fmt.Fprintf(os.Stdout, "Response from `PaymentsApi.ConnectorsStripeTransfer`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiConnectorsStripeTransferRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **stripeTransferRequest** | [**StripeTransferRequest**](StripeTransferRequest.md) |  | 

### Return type

**map[string]interface{}**

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetConnectorTask

> TaskResponse GetConnectorTask(ctx, connector, taskId).Execute()

Read a specific task of the connector



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
    connector := client.Connector("STRIPE") // Connector | The name of the connector.
    taskId := "task1" // string | The task ID.

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.PaymentsApi.GetConnectorTask(context.Background(), connector, taskId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.GetConnectorTask``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetConnectorTask`: TaskResponse
    fmt.Fprintf(os.Stdout, "Response from `PaymentsApi.GetConnectorTask`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**connector** | [**Connector**](.md) | The name of the connector. | 
**taskId** | **string** | The task ID. | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetConnectorTaskRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------



### Return type

[**TaskResponse**](TaskResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## GetPayment

> PaymentResponse GetPayment(ctx, paymentId).Execute()

Get a payment

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
    paymentId := "XXX" // string | The payment ID.

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.PaymentsApi.GetPayment(context.Background(), paymentId).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.GetPayment``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `GetPayment`: PaymentResponse
    fmt.Fprintf(os.Stdout, "Response from `PaymentsApi.GetPayment`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**paymentId** | **string** | The payment ID. | 

### Other Parameters

Other parameters are passed through a pointer to a apiGetPaymentRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**PaymentResponse**](PaymentResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## InstallConnector

> InstallConnector(ctx, connector).ConnectorConfig(connectorConfig).Execute()

Install a connector



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
    connector := client.Connector("STRIPE") // Connector | The name of the connector.
    connectorConfig := client.ConnectorConfig{BankingCircleConfig: client.NewBankingCircleConfig("XXX", "XXX", "XXX", "XXX")} // ConnectorConfig | 

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.PaymentsApi.InstallConnector(context.Background(), connector).ConnectorConfig(connectorConfig).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.InstallConnector``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**connector** | [**Connector**](.md) | The name of the connector. | 

### Other Parameters

Other parameters are passed through a pointer to a apiInstallConnectorRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **connectorConfig** | [**ConnectorConfig**](ConnectorConfig.md) |  | 

### Return type

 (empty response body)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: Not defined

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListAllConnectors

> ConnectorsResponse ListAllConnectors(ctx).Execute()

List all installed connectors



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
    resp, r, err := apiClient.PaymentsApi.ListAllConnectors(context.Background()).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.ListAllConnectors``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListAllConnectors`: ConnectorsResponse
    fmt.Fprintf(os.Stdout, "Response from `PaymentsApi.ListAllConnectors`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiListAllConnectorsRequest struct via the builder pattern


### Return type

[**ConnectorsResponse**](ConnectorsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListConfigsAvailableConnectors

> ConnectorsConfigsResponse ListConfigsAvailableConnectors(ctx).Execute()

List the configs of each available connector



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
    resp, r, err := apiClient.PaymentsApi.ListConfigsAvailableConnectors(context.Background()).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.ListConfigsAvailableConnectors``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListConfigsAvailableConnectors`: ConnectorsConfigsResponse
    fmt.Fprintf(os.Stdout, "Response from `PaymentsApi.ListConfigsAvailableConnectors`: %v\n", resp)
}
```

### Path Parameters

This endpoint does not need any parameter.

### Other Parameters

Other parameters are passed through a pointer to a apiListConfigsAvailableConnectorsRequest struct via the builder pattern


### Return type

[**ConnectorsConfigsResponse**](ConnectorsConfigsResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListConnectorTasks

> TasksCursor ListConnectorTasks(ctx, connector).PageSize(pageSize).Cursor(cursor).Execute()

List tasks from a connector



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
    connector := client.Connector("STRIPE") // Connector | The name of the connector.
    pageSize := int64(100) // int64 | The maximum number of results to return per page.  (optional) (default to 15)
    cursor := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.PaymentsApi.ListConnectorTasks(context.Background(), connector).PageSize(pageSize).Cursor(cursor).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.ListConnectorTasks``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListConnectorTasks`: TasksCursor
    fmt.Fprintf(os.Stdout, "Response from `PaymentsApi.ListConnectorTasks`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**connector** | [**Connector**](.md) | The name of the connector. | 

### Other Parameters

Other parameters are passed through a pointer to a apiListConnectorTasksRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------

 **pageSize** | **int64** | The maximum number of results to return per page.  | [default to 15]
 **cursor** | **string** | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | 

### Return type

[**TasksCursor**](TasksCursor.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ListPayments

> PaymentsCursor ListPayments(ctx).PageSize(pageSize).Cursor(cursor).Sort(sort).Execute()

List payments

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
    pageSize := int64(100) // int64 | The maximum number of results to return per page.  (optional) (default to 15)
    cursor := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
    sort := []string{"Inner_example"} // []string | Fields used to sort payments (default is date:desc). (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.PaymentsApi.ListPayments(context.Background()).PageSize(pageSize).Cursor(cursor).Sort(sort).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.ListPayments``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ListPayments`: PaymentsCursor
    fmt.Fprintf(os.Stdout, "Response from `PaymentsApi.ListPayments`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiListPaymentsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pageSize** | **int64** | The maximum number of results to return per page.  | [default to 15]
 **cursor** | **string** | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | 
 **sort** | **[]string** | Fields used to sort payments (default is date:desc). | 

### Return type

[**PaymentsCursor**](PaymentsCursor.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## PaymentslistAccounts

> AccountsCursor PaymentslistAccounts(ctx).PageSize(pageSize).Cursor(cursor).Sort(sort).Execute()

List accounts

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
    pageSize := int64(100) // int64 | The maximum number of results to return per page.  (optional) (default to 15)
    cursor := "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==" // string | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  (optional)
    sort := []string{"Inner_example"} // []string | Fields used to sort payments (default is date:desc). (optional)

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.PaymentsApi.PaymentslistAccounts(context.Background()).PageSize(pageSize).Cursor(cursor).Sort(sort).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.PaymentslistAccounts``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `PaymentslistAccounts`: AccountsCursor
    fmt.Fprintf(os.Stdout, "Response from `PaymentsApi.PaymentslistAccounts`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiPaymentslistAccountsRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **pageSize** | **int64** | The maximum number of results to return per page.  | [default to 15]
 **cursor** | **string** | Parameter used in pagination requests. Maximum page size is set to 15. Set to the value of next for the next page of results. Set to the value of previous for the previous page of results. No other parameters can be set when this parameter is set.  | 
 **sort** | **[]string** | Fields used to sort payments (default is date:desc). | 

### Return type

[**AccountsCursor**](AccountsCursor.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ReadConnectorConfig

> ConnectorConfigResponse ReadConnectorConfig(ctx, connector).Execute()

Read the config of a connector



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
    connector := client.Connector("STRIPE") // Connector | The name of the connector.

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.PaymentsApi.ReadConnectorConfig(context.Background(), connector).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.ReadConnectorConfig``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `ReadConnectorConfig`: ConnectorConfigResponse
    fmt.Fprintf(os.Stdout, "Response from `PaymentsApi.ReadConnectorConfig`: %v\n", resp)
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**connector** | [**Connector**](.md) | The name of the connector. | 

### Other Parameters

Other parameters are passed through a pointer to a apiReadConnectorConfigRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------


### Return type

[**ConnectorConfigResponse**](ConnectorConfigResponse.md)

### Authorization

[Authorization](../README.md#Authorization)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## ResetConnector

> ResetConnector(ctx, connector).Execute()

Reset a connector



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
    connector := client.Connector("STRIPE") // Connector | The name of the connector.

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.PaymentsApi.ResetConnector(context.Background(), connector).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.ResetConnector``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**connector** | [**Connector**](.md) | The name of the connector. | 

### Other Parameters

Other parameters are passed through a pointer to a apiResetConnectorRequest struct via the builder pattern


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


## UninstallConnector

> UninstallConnector(ctx, connector).Execute()

Uninstall a connector



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
    connector := client.Connector("STRIPE") // Connector | The name of the connector.

    configuration := client.NewConfiguration()
    apiClient := client.NewAPIClient(configuration)
    resp, r, err := apiClient.PaymentsApi.UninstallConnector(context.Background(), connector).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `PaymentsApi.UninstallConnector``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
}
```

### Path Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
**ctx** | **context.Context** | context for authentication, logging, cancellation, deadlines, tracing, etc.
**connector** | [**Connector**](.md) | The name of the connector. | 

### Other Parameters

Other parameters are passed through a pointer to a apiUninstallConnectorRequest struct via the builder pattern


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

