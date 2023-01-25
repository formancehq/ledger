<a name="__pageTop"></a>
# Formance.apis.tags.transactions_api.TransactionsApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**add_metadata_on_transaction**](#add_metadata_on_transaction) | **post** /api/ledger/{ledger}/transactions/{txid}/metadata | Set the metadata of a transaction by its ID
[**count_transactions**](#count_transactions) | **head** /api/ledger/{ledger}/transactions | Count the transactions from a ledger
[**create_transaction**](#create_transaction) | **post** /api/ledger/{ledger}/transactions | Create a new transaction to a ledger
[**create_transactions**](#create_transactions) | **post** /api/ledger/{ledger}/transactions/batch | Create a new batch of transactions to a ledger
[**get_transaction**](#get_transaction) | **get** /api/ledger/{ledger}/transactions/{txid} | Get transaction from a ledger by its ID
[**list_transactions**](#list_transactions) | **get** /api/ledger/{ledger}/transactions | List transactions from a ledger
[**revert_transaction**](#revert_transaction) | **post** /api/ledger/{ledger}/transactions/{txid}/revert | Revert a ledger transaction by its ID

# **add_metadata_on_transaction**
<a name="add_metadata_on_transaction"></a>
> add_metadata_on_transaction(ledgertxid)

Set the metadata of a transaction by its ID

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import transactions_api
from Formance.model.error_response import ErrorResponse
from Formance.model.ledger_metadata import LedgerMetadata
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
    api_instance = transactions_api.TransactionsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
        'txid': 1234,
    }
    try:
        # Set the metadata of a transaction by its ID
        api_response = api_instance.add_metadata_on_transaction(
            path_params=path_params,
        )
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->add_metadata_on_transaction: %s\n" % e)

    # example passing only optional values
    path_params = {
        'ledger': "ledger001",
        'txid': 1234,
    }
    body = LedgerMetadata(
        key=None,
    )
    try:
        # Set the metadata of a transaction by its ID
        api_response = api_instance.add_metadata_on_transaction(
            path_params=path_params,
            body=body,
        )
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->add_metadata_on_transaction: %s\n" % e)
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
[**LedgerMetadata**](../../models/LedgerMetadata.md) |  | 


### path_params
#### RequestPathParams

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
ledger | LedgerSchema | | 
txid | TxidSchema | | 

# LedgerSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# TxidSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
204 | [ApiResponseFor204](#add_metadata_on_transaction.ApiResponseFor204) | No Content
default | [ApiResponseForDefault](#add_metadata_on_transaction.ApiResponseForDefault) | Error

#### add_metadata_on_transaction.ApiResponseFor204
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[] |  |
headers | Unset | headers were not defined |

#### add_metadata_on_transaction.ApiResponseForDefault
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

# **count_transactions**
<a name="count_transactions"></a>
> count_transactions(ledger)

Count the transactions from a ledger

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import transactions_api
from Formance.model.error_response import ErrorResponse
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
    api_instance = transactions_api.TransactionsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
    }
    query_params = {
    }
    try:
        # Count the transactions from a ledger
        api_response = api_instance.count_transactions(
            path_params=path_params,
            query_params=query_params,
        )
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->count_transactions: %s\n" % e)

    # example passing only optional values
    path_params = {
        'ledger': "ledger001",
    }
    query_params = {
        'reference': "ref:001",
        'account': "users:001",
        'source': "users:001",
        'destination': "users:001",
        'startTime': "1970-01-01T00:00:00.00Z",
        'start_time': "1970-01-01T00:00:00.00Z",
        'endTime': "1970-01-01T00:00:00.00Z",
        'end_time': "1970-01-01T00:00:00.00Z",
        'metadata': dict(),
    }
    try:
        # Count the transactions from a ledger
        api_response = api_instance.count_transactions(
            path_params=path_params,
            query_params=query_params,
        )
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->count_transactions: %s\n" % e)
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
reference | ReferenceSchema | | optional
account | AccountSchema | | optional
source | SourceSchema | | optional
destination | DestinationSchema | | optional
startTime | StartTimeSchema | | optional
start_time | StartTimeSchema | | optional
endTime | EndTimeSchema | | optional
end_time | EndTimeSchema | | optional
metadata | MetadataSchema | | optional


# ReferenceSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# AccountSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# SourceSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# DestinationSchema

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

# MetadataSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

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
200 | [ApiResponseFor200](#count_transactions.ApiResponseFor200) | OK
default | [ApiResponseForDefault](#count_transactions.ApiResponseForDefault) | Error

#### count_transactions.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | Unset | body was not defined |
headers | ResponseHeadersFor200 |  |
#### ResponseHeadersFor200

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
Count | CountSchema | | optional

# CountSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer


#### count_transactions.ApiResponseForDefault
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

# **create_transaction**
<a name="create_transaction"></a>
> TransactionsResponse create_transaction(ledgerpost_transaction)

Create a new transaction to a ledger

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import transactions_api
from Formance.model.error_response import ErrorResponse
from Formance.model.post_transaction import PostTransaction
from Formance.model.transactions_response import TransactionsResponse
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
    api_instance = transactions_api.TransactionsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
    }
    query_params = {
    }
    body = PostTransaction(
        timestamp="1970-01-01T00:00:00.00Z",
        postings=[
            Posting(
                amount=100,
                asset="COIN",
                destination="users:002",
                source="users:001",
            )
        ],
        script=dict(
            plain="vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
            vars=dict(),
        ),
        reference="ref:001",
        metadata=LedgerMetadata(
            key=None,
        ),
    )
    try:
        # Create a new transaction to a ledger
        api_response = api_instance.create_transaction(
            path_params=path_params,
            query_params=query_params,
            body=body,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->create_transaction: %s\n" % e)

    # example passing only optional values
    path_params = {
        'ledger': "ledger001",
    }
    query_params = {
        'preview': True,
    }
    body = PostTransaction(
        timestamp="1970-01-01T00:00:00.00Z",
        postings=[
            Posting(
                amount=100,
                asset="COIN",
                destination="users:002",
                source="users:001",
            )
        ],
        script=dict(
            plain="vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
            vars=dict(),
        ),
        reference="ref:001",
        metadata=LedgerMetadata(
            key=None,
        ),
    )
    try:
        # Create a new transaction to a ledger
        api_response = api_instance.create_transaction(
            path_params=path_params,
            query_params=query_params,
            body=body,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->create_transaction: %s\n" % e)
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
[**PostTransaction**](../../models/PostTransaction.md) |  | 


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
200 | [ApiResponseFor200](#create_transaction.ApiResponseFor200) | OK
default | [ApiResponseForDefault](#create_transaction.ApiResponseForDefault) | Error

#### create_transaction.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**TransactionsResponse**](../../models/TransactionsResponse.md) |  | 


#### create_transaction.ApiResponseForDefault
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

# **create_transactions**
<a name="create_transactions"></a>
> TransactionsResponse create_transactions(ledgertransactions)

Create a new batch of transactions to a ledger

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import transactions_api
from Formance.model.error_response import ErrorResponse
from Formance.model.transactions_response import TransactionsResponse
from Formance.model.transactions import Transactions
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
    api_instance = transactions_api.TransactionsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
    }
    body = Transactions(
        transactions=[
            TransactionData(
                postings=[
                    Posting(
                        amount=100,
                        asset="COIN",
                        destination="users:002",
                        source="users:001",
                    )
                ],
                reference="ref:001",
                metadata=LedgerMetadata(
                    key=None,
                ),
                timestamp="1970-01-01T00:00:00.00Z",
            )
        ],
    )
    try:
        # Create a new batch of transactions to a ledger
        api_response = api_instance.create_transactions(
            path_params=path_params,
            body=body,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->create_transactions: %s\n" % e)
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
[**Transactions**](../../models/Transactions.md) |  | 


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
200 | [ApiResponseFor200](#create_transactions.ApiResponseFor200) | OK
default | [ApiResponseForDefault](#create_transactions.ApiResponseForDefault) | Error

#### create_transactions.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**TransactionsResponse**](../../models/TransactionsResponse.md) |  | 


#### create_transactions.ApiResponseForDefault
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

# **get_transaction**
<a name="get_transaction"></a>
> TransactionResponse get_transaction(ledgertxid)

Get transaction from a ledger by its ID

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import transactions_api
from Formance.model.transaction_response import TransactionResponse
from Formance.model.error_response import ErrorResponse
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
    api_instance = transactions_api.TransactionsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
        'txid': 1234,
    }
    try:
        # Get transaction from a ledger by its ID
        api_response = api_instance.get_transaction(
            path_params=path_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->get_transaction: %s\n" % e)
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
txid | TxidSchema | | 

# LedgerSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# TxidSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#get_transaction.ApiResponseFor200) | OK
default | [ApiResponseForDefault](#get_transaction.ApiResponseForDefault) | Error

#### get_transaction.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**TransactionResponse**](../../models/TransactionResponse.md) |  | 


#### get_transaction.ApiResponseForDefault
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

# **list_transactions**
<a name="list_transactions"></a>
> TransactionsCursorResponse list_transactions(ledger)

List transactions from a ledger

List transactions from a ledger, sorted by txid in descending order.

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import transactions_api
from Formance.model.error_response import ErrorResponse
from Formance.model.transactions_cursor_response import TransactionsCursorResponse
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
    api_instance = transactions_api.TransactionsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
    }
    query_params = {
    }
    try:
        # List transactions from a ledger
        api_response = api_instance.list_transactions(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->list_transactions: %s\n" % e)

    # example passing only optional values
    path_params = {
        'ledger': "ledger001",
    }
    query_params = {
        'pageSize': 100,
        'page_size': 100,
        'after': "1234",
        'reference': "ref:001",
        'account': "users:001",
        'source': "users:001",
        'destination': "users:001",
        'startTime': "1970-01-01T00:00:00.00Z",
        'start_time': "1970-01-01T00:00:00.00Z",
        'endTime': "1970-01-01T00:00:00.00Z",
        'end_time': "1970-01-01T00:00:00.00Z",
        'cursor': "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
        'pagination_token': "aHR0cHM6Ly9nLnBhZ2UvTmVrby1SYW1lbj9zaGFyZQ==",
        'metadata': dict(),
    }
    try:
        # List transactions from a ledger
        api_response = api_instance.list_transactions(
            path_params=path_params,
            query_params=query_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->list_transactions: %s\n" % e)
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
reference | ReferenceSchema | | optional
account | AccountSchema | | optional
source | SourceSchema | | optional
destination | DestinationSchema | | optional
startTime | StartTimeSchema | | optional
start_time | StartTimeSchema | | optional
endTime | EndTimeSchema | | optional
end_time | EndTimeSchema | | optional
cursor | CursorSchema | | optional
pagination_token | PaginationTokenSchema | | optional
metadata | MetadataSchema | | optional


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

# ReferenceSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# AccountSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# SourceSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# DestinationSchema

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

# MetadataSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
dict, frozendict.frozendict,  | frozendict.frozendict,  |  | 

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
200 | [ApiResponseFor200](#list_transactions.ApiResponseFor200) | OK
default | [ApiResponseForDefault](#list_transactions.ApiResponseForDefault) | Error

#### list_transactions.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**TransactionsCursorResponse**](../../models/TransactionsCursorResponse.md) |  | 


#### list_transactions.ApiResponseForDefault
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

# **revert_transaction**
<a name="revert_transaction"></a>
> TransactionResponse revert_transaction(ledgertxid)

Revert a ledger transaction by its ID

### Example

* OAuth Authentication (Authorization):
```python
import Formance
from Formance.apis.tags import transactions_api
from Formance.model.transaction_response import TransactionResponse
from Formance.model.error_response import ErrorResponse
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
    api_instance = transactions_api.TransactionsApi(api_client)

    # example passing only required values which don't have defaults set
    path_params = {
        'ledger': "ledger001",
        'txid': 1234,
    }
    try:
        # Revert a ledger transaction by its ID
        api_response = api_instance.revert_transaction(
            path_params=path_params,
        )
        pprint(api_response)
    except Formance.ApiException as e:
        print("Exception when calling TransactionsApi->revert_transaction: %s\n" % e)
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
txid | TxidSchema | | 

# LedgerSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
str,  | str,  |  | 

# TxidSchema

## Model Type Info
Input Type | Accessed Type | Description | Notes
------------ | ------------- | ------------- | -------------
decimal.Decimal, int,  | decimal.Decimal,  |  | value must be a 64 bit integer

### Return Types, Responses

Code | Class | Description
------------- | ------------- | -------------
n/a | api_client.ApiResponseWithoutDeserialization | When skip_deserialization is True this response is returned
200 | [ApiResponseFor200](#revert_transaction.ApiResponseFor200) | OK
default | [ApiResponseForDefault](#revert_transaction.ApiResponseForDefault) | Error

#### revert_transaction.ApiResponseFor200
Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
response | urllib3.HTTPResponse | Raw response |
body | typing.Union[SchemaFor200ResponseBodyApplicationJson, ] |  |
headers | Unset | headers were not defined |

# SchemaFor200ResponseBodyApplicationJson
Type | Description  | Notes
------------- | ------------- | -------------
[**TransactionResponse**](../../models/TransactionResponse.md) |  | 


#### revert_transaction.ApiResponseForDefault
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

