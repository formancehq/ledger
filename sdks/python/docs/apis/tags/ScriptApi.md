<a name="__pageTop"></a>
# Formance.apis.tags.script_api.ScriptApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**run_script**](#run_script) | **post** /api/ledger/{ledger}/script | Execute a Numscript

# **run_script**
<a name="run_script"></a>
> ScriptResponse run_script(ledgerscript)

Execute a Numscript

This route is deprecated, and has been merged into `POST /{ledger}/transactions`. 

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import script_api
from Formance.model.script import Script
from Formance.model.script_response import ScriptResponse
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
    api_instance = script_api.ScriptApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
    }
    query_params = {
    }
    body = Script(
        plain="vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
        vars=dict(),
        reference="order_1234",
        metadata=LedgerMetadata(
            key=None,
        ),
    )
    try:
        # Execute a Numscript
        api_response = api_instance.run_script(
            path_params=path_params,
            query_params=query_params,
            body=body,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling ScriptApi->run_script: %s\n" % e)

    # example passing only optional values
    path_params = {
        'ledger': "ledger001",
    }
    query_params = {
        'preview': True,
    }
    body = Script(
        plain="vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
        vars=dict(),
        reference="order_1234",
        metadata=LedgerMetadata(
            key=None,
        ),
    )
    try:
        # Execute a Numscript
        api_response = api_instance.run_script(
            path_params=path_params,
            query_params=query_params,
            body=body,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling ScriptApi->run_script: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
query_params | RequestQueryParams | |
path_params | RequestPathParams | |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**Script**](../../models/Script.md) |  | 


### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
preview | PreviewSchema | | optional


# PreviewSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
bool,  | BoolClass,  |  | 

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
200 | [ApiResponseFor200](#run_script.ApiResponseFor200) | On success, it will return a 200 status code, and the resulting transaction under the &#x60;transaction&#x60; field.  On failure, it will also return a 200 status code, and the following fields:   - &#x60;details&#x60;: contains a URL. When there is an error parsing Numscript, the result can be difficult to read—the provided URL will render the error in an easy-to-read format.   - &#x60;errorCode&#x60; and &#x60;error_code&#x60; (deprecated): contains the string code of the error   - &#x60;errorMessage&#x60; and &#x60;error_message&#x60; (deprecated): contains a human-readable indication of what went wrong, for example that an account had insufficient funds, or that there was an error in the provided Numscript. 

#### run_script.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ScriptResponse**](../../models/ScriptResponse.md) |  | 


### Authorization

[Authorization](../../../README.md#Authorization)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

