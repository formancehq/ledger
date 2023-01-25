<a name="__pageTop"></a>
# Formance.apis.tags.logs_api.LogsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**list_logs**](#list_logs) | **get** /api/ledger/{ledger}/log | List the logs from a ledger

# **list_logs**
<a name="list_logs"></a>
> LogsCursorResponse list_logs(ledger)

List the logs from a ledger

List the logs from a ledger, sorted by ID in descending order.

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import logs_api
from Formance.model.error_response import ErrorResponse
from Formance.model.logs_cursor_response import LogsCursorResponse
from pprint import pprint
# Defining the host is optional and defaults to http://localhost
# See configuration.py for a list of all supported configuration parameters.
configuration = Formance.Configuration(
    host = "http://localhost"
)

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure OAuth2 access token for authorization: Authorization
configuration = Formance.Configuration(
    host = "http://localhost"
)
configuration.access_token = 'YOUR_ACCESS_TOKEN'
# Enter a context with an instance of the API client
with Formance.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = logs_api.LogsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
    }
    query_params = {
    }
    try:
        # List the logs from a ledger
        api_response = api_instance.list_logs(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling LogsApi->list_logs: %s\n" % e)

    # example passing only optional values
    path_params = {
        'ledger': "ledger001",
    }
    query_params = {
        'pageSize': 100,
        'page_size': 100,
        'after': "1234",
        'startTime': "1970-01-01T00:00:00.00Z",
        'start_time': "1970-01-01T00:00:00.00Z",
        'endTime': "1970-01-01T00:00:00.00Z",
        'end_time': "1970-01-01T00:00:00.00Z",
        'cursor': "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
        'pagination_token': "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
    }
    try:
        # List the logs from a ledger
        api_response = api_instance.list_logs(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling LogsApi->list_logs: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
pageSize | PageSizeSchema | | optional
page_size | PageSizeSchema | | optional
after | AfterSchema | | optional
startTime | StartTimeSchema | | optional
start_time | StartTimeSchema | | optional
endTime | EndTimeSchema | | optional
end_time | EndTimeSchema | | optional
cursor | CursorSchema | | optional
pagination_token | PaginationTokenSchema | | optional


# PageSizeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | if omitted the server will use the default value of 15value must be a 64 bit integer

# PageSizeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | if omitted the server will use the default value of 15value must be a 64 bit integer

# AfterSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# StartTimeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str, datetime,  | str,  |  | value must conform to RFC-3339 date-time

# StartTimeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str, datetime,  | str,  |  | value must conform to RFC-3339 date-time

# EndTimeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str, datetime,  | str,  |  | value must conform to RFC-3339 date-time

# EndTimeSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str, datetime,  | str,  |  | value must conform to RFC-3339 date-time

# CursorSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# PaginationTokenSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
ledger | LedgerSchema | | 

# LedgerSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#list_logs.ApiResponseFor200) | OK
default | [ApiResponseForDefault](#list_logs.ApiResponseForDefault) | Error

#### list_logs.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**LogsCursorResponse**](../../models/LogsCursorResponse.md) |  | 


#### list_logs.ApiResponseForDefault
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor0ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor0ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ErrorResponse**](../../models/ErrorResponse.md) |  | 


### Authorization

[Authorization](../../../README.md#Authorization)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

