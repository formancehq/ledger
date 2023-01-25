<a name="__pageTop"></a>
# Formance.apis.tags.mapping_api.MappingApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**get_mapping**](#get_mapping) | **get** /api/ledger/{ledger}/mapping | Get the mapping of a ledger
[**update_mapping**](#update_mapping) | **put** /api/ledger/{ledger}/mapping | Update the mapping of a ledger

# **get_mapping**
<a name="get_mapping"></a>
> MappingResponse get_mapping(ledger)

Get the mapping of a ledger

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import mapping_api
from Formance.model.error_response import ErrorResponse
from Formance.model.mapping_response import MappingResponse
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
    api_instance = mapping_api.MappingApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
    }
    try:
        # Get the mapping of a ledger
        api_response = api_instance.get_mapping(
            path_params=path_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling MappingApi->get_mapping: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

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
200 | [ApiResponseFor200](#get_mapping.ApiResponseFor200) | OK
default | [ApiResponseForDefault](#get_mapping.ApiResponseForDefault) | Error

#### get_mapping.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**MappingResponse**](../../models/MappingResponse.md) |  | 


#### get_mapping.ApiResponseForDefault
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

# **update_mapping**
<a name="update_mapping"></a>
> MappingResponse update_mapping(ledgermapping)

Update the mapping of a ledger

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import mapping_api
from Formance.model.mapping import Mapping
from Formance.model.error_response import ErrorResponse
from Formance.model.mapping_response import MappingResponse
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
    api_instance = mapping_api.MappingApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
    }
    body = Mapping(
        contracts=[
            Contract(
                account="users:001",
                expr=dict(),
            )
        ],
    )
    try:
        # Update the mapping of a ledger
        api_response = api_instance.update_mapping(
            path_params=path_params,
            body=body,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling MappingApi->update_mapping: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
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
[**Mapping**](../../models/Mapping.md) |  | 


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
200 | [ApiResponseFor200](#update_mapping.ApiResponseFor200) | OK
default | [ApiResponseForDefault](#update_mapping.ApiResponseForDefault) | Error

#### update_mapping.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**MappingResponse**](../../models/MappingResponse.md) |  | 


#### update_mapping.ApiResponseForDefault
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

