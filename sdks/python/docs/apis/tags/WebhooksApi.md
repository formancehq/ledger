<a name="__pageTop"></a>
# Formance.apis.tags.webhooks_api.WebhooksApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**activate_config**](#activate_config) | **put** /api/webhooks/configs/{id}/activate | Activate one config
[**change_config_secret**](#change_config_secret) | **put** /api/webhooks/configs/{id}/secret/change | Change the signing secret of a config
[**deactivate_config**](#deactivate_config) | **put** /api/webhooks/configs/{id}/deactivate | Deactivate one config
[**delete_config**](#delete_config) | **delete** /api/webhooks/configs/{id} | Delete one config
[**get_many_configs**](#get_many_configs) | **get** /api/webhooks/configs | Get many configs
[**insert_config**](#insert_config) | **post** /api/webhooks/configs | Insert a new config
[**test_config**](#test_config) | **get** /api/webhooks/configs/{id}/test | Test one config

# **activate_config**
<a name="activate_config"></a>
> ConfigResponse activate_config(id)

Activate one config

Activate a webhooks config by ID, to start receiving webhooks to its endpoint.

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import webhooks_api
from Formance.model.config_response import ConfigResponse
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
    api_instance = webhooks_api.WebhooksApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'id': "4997257d-dfb6-445b-929c-cbe2ab182818",
    }
    try:
        # Activate one config
        api_response = api_instance.activate_config(
            path_params=path_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling WebhooksApi->activate_config: %s\n" % e)
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
id | IdSchema | | 

# IdSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#activate_config.ApiResponseFor200) | Config successfully activated.
304 | [ApiResponseFor304](#activate_config.ApiResponseFor304) | Config not modified, was already activated.

#### activate_config.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ConfigResponse**](../../models/ConfigResponse.md) |  | 


#### activate_config.ApiResponseFor304
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[] |  |
headers | Unset | headers were not defined |

### Authorization

[Authorization](../../../README.md#Authorization)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **change_config_secret**
<a name="change_config_secret"></a>
> ConfigResponse change_config_secret(id)

Change the signing secret of a config

Change the signing secret of the endpoint of a webhooks config.  If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding) 

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import webhooks_api
from Formance.model.config_response import ConfigResponse
from Formance.model.config_change_secret import ConfigChangeSecret
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
    api_instance = webhooks_api.WebhooksApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'id': "4997257d-dfb6-445b-929c-cbe2ab182818",
    }
    try:
        # Change the signing secret of a config
        api_response = api_instance.change_config_secret(
            path_params=path_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling WebhooksApi->change_config_secret: %s\n" % e)

    # example passing only optional values
    path_params = {
        'id': "4997257d-dfb6-445b-929c-cbe2ab182818",
    }
    body = ConfigChangeSecret(
        secret="V0bivxRWveaoz08afqjU6Ko/jwO0Cb+3",
    )
    try:
        # Change the signing secret of a config
        api_response = api_instance.change_config_secret(
            path_params=path_params,
            body=body,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling WebhooksApi->change_config_secret: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson, Unset] | optional, default is unset |
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
[**ConfigChangeSecret**](../../models/ConfigChangeSecret.md) |  | 


### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
id | IdSchema | | 

# IdSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#change_config_secret.ApiResponseFor200) | Secret successfully changed.

#### change_config_secret.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ConfigResponse**](../../models/ConfigResponse.md) |  | 


### Authorization

[Authorization](../../../README.md#Authorization)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **deactivate_config**
<a name="deactivate_config"></a>
> ConfigResponse deactivate_config(id)

Deactivate one config

Deactivate a webhooks config by ID, to stop receiving webhooks to its endpoint.

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import webhooks_api
from Formance.model.config_response import ConfigResponse
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
    api_instance = webhooks_api.WebhooksApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'id': "4997257d-dfb6-445b-929c-cbe2ab182818",
    }
    try:
        # Deactivate one config
        api_response = api_instance.deactivate_config(
            path_params=path_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling WebhooksApi->deactivate_config: %s\n" % e)
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
id | IdSchema | | 

# IdSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#deactivate_config.ApiResponseFor200) | Config successfully deactivated.
304 | [ApiResponseFor304](#deactivate_config.ApiResponseFor304) | Config not modified, was already deactivated.

#### deactivate_config.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ConfigResponse**](../../models/ConfigResponse.md) |  | 


#### deactivate_config.ApiResponseFor304
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[] |  |
headers | Unset | headers were not defined |

### Authorization

[Authorization](../../../README.md#Authorization)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **delete_config**
<a name="delete_config"></a>
> delete_config(id)

Delete one config

Delete a webhooks config by ID.

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import webhooks_api
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
    api_instance = webhooks_api.WebhooksApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'id': "4997257d-dfb6-445b-929c-cbe2ab182818",
    }
    try:
        # Delete one config
        api_response = api_instance.delete_config(
            path_params=path_params,
        )
    except Formance.ApiException as e:
        print("Exception when calling WebhooksApi->delete_config: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
path_params | RequestPathParams | |
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
id | IdSchema | | 

# IdSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#delete_config.ApiResponseFor200) | Config successfully deleted.

#### delete_config.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[] |  |
headers | Unset | headers were not defined |

### Authorization

[Authorization](../../../README.md#Authorization)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **get_many_configs**
<a name="get_many_configs"></a>
> ConfigsResponse get_many_configs()

Get many configs

Sorted by updated date descending

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import webhooks_api
from Formance.model.configs_response import ConfigsResponse
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
    api_instance = webhooks_api.WebhooksApi(api_client)

    # example passing only optional values
    query_params = {
        'id': "4997257d-dfb6-445b-929c-cbe2ab182818",
        'endpoint': "https://example.com",
    }
    try:
        # Get many configs
        api_response = api_instance.get_many_configs(
            query_params=query_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling WebhooksApi->get_many_configs: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
query_params | RequestQueryParams | |
accept_content_types | typing.Tuple[str] | default is ('application/json', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### query_params
#### RequestQueryParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
id | IdSchema | | optional
endpoint | EndpointSchema | | optional


# IdSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# EndpointSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#get_many_configs.ApiResponseFor200) | OK

#### get_many_configs.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ConfigsResponse**](../../models/ConfigsResponse.md) |  | 


### Authorization

[Authorization](../../../README.md#Authorization)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **insert_config**
<a name="insert_config"></a>
> ConfigResponse insert_config(config_user)

Insert a new config

Insert a new webhooks config.  The endpoint should be a valid https URL and be unique.  The secret is the endpoint's verification secret. If not passed or empty, a secret is automatically generated. The format is a random string of bytes of size 24, base64 encoded. (larger size after encoding)  All eventTypes are converted to lower-case when inserted. 

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import webhooks_api
from Formance.model.config_user import ConfigUser
from Formance.model.config_response import ConfigResponse
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
    api_instance = webhooks_api.WebhooksApi(api_client)

    # example passing only required values which don't have defaults set
    body = ConfigUser(
        endpoint="https://example.com",
        secret="V0bivxRWveaoz08afqjU6Ko/jwO0Cb+3",
        event_types=["TYPE1","TYPE2"],
    )
    try:
        # Insert a new config
        api_response = api_instance.insert_config(
            body=body,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling WebhooksApi->insert_config: %s\n" % e)
```
### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
body | typing.Union[SchemaForRequestBodyApplicationJson] | required |
content_type | str | optional, default is 'application/json' | Selects the schema and serialization of the request body
accept_content_types | typing.Tuple[str] | default is ('application/json', 'text/plain', ) | Tells the server the content type(s) that are accepted by the client
stream | bool | default is False | if True then the response.content will be streamed and loaded from a file like object. When downloading a file, set this to True to force the code to deserialize the content to a FileSchema file
timeout | typing.Optional[typing.Union[int, typing.Tuple]] | default is None | the timeout used by the rest client
skip_deserialization | bool | default is False | when True, headers and body will be unset and an instance of api_client.ApiResponseWithoutDeserialization will be returned

### body

# SchemaForRequestBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ConfigUser**](../../models/ConfigUser.md) |  | 


### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#insert_config.ApiResponseFor200) | Config created successfully.
400 | [ApiResponseFor400](#insert_config.ApiResponseFor400) | Bad Request

#### insert_config.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**ConfigResponse**](../../models/ConfigResponse.md) |  | 


#### insert_config.ApiResponseFor400
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor400ResponseBodyTextPlain, ] |  |
headers | Unset | headers were not defined |

# SchemaFor400ResponseBodyTextPlain

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Authorization

[Authorization](../../../README.md#Authorization)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

# **test_config**
<a name="test_config"></a>
> AttemptResponse test_config(id)

Test one config

Test a config by sending a webhook to its endpoint.

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import webhooks_api
from Formance.model.attempt_response import AttemptResponse
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
    api_instance = webhooks_api.WebhooksApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'id': "4997257d-dfb6-445b-929c-cbe2ab182818",
    }
    try:
        # Test one config
        api_response = api_instance.test_config(
            path_params=path_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling WebhooksApi->test_config: %s\n" % e)
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
id | IdSchema | | 

# IdSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#test_config.ApiResponseFor200) | OK

#### test_config.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**AttemptResponse**](../../models/AttemptResponse.md) |  | 


### Authorization

[Authorization](../../../README.md#Authorization)

[[Back to top]](#__pageTop) [[Back to API list]](../../../README.md#documentation-for-api-endpoints) [[Back to Model list]](../../../README.md#documentation-for-models) [[Back to README]](../../../README.md)

