# GetTransaction404Response

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ErrorCode** | **string** |  | 
**ErrorMessage** | Pointer to **string** |  | [optional] 

## Methods

### NewGetTransaction404Response

`func NewGetTransaction404Response(errorCode string, ) *GetTransaction404Response`

NewGetTransaction404Response instantiates a new GetTransaction404Response object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewGetTransaction404ResponseWithDefaults

`func NewGetTransaction404ResponseWithDefaults() *GetTransaction404Response`

NewGetTransaction404ResponseWithDefaults instantiates a new GetTransaction404Response object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetErrorCode

`func (o *GetTransaction404Response) GetErrorCode() string`

GetErrorCode returns the ErrorCode field if non-nil, zero value otherwise.

### GetErrorCodeOk

`func (o *GetTransaction404Response) GetErrorCodeOk() (*string, bool)`

GetErrorCodeOk returns a tuple with the ErrorCode field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorCode

`func (o *GetTransaction404Response) SetErrorCode(v string)`

SetErrorCode sets ErrorCode field to given value.


### GetErrorMessage

`func (o *GetTransaction404Response) GetErrorMessage() string`

GetErrorMessage returns the ErrorMessage field if non-nil, zero value otherwise.

### GetErrorMessageOk

`func (o *GetTransaction404Response) GetErrorMessageOk() (*string, bool)`

GetErrorMessageOk returns a tuple with the ErrorMessage field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorMessage

`func (o *GetTransaction404Response) SetErrorMessage(v string)`

SetErrorMessage sets ErrorMessage field to given value.

### HasErrorMessage

`func (o *GetTransaction404Response) HasErrorMessage() bool`

HasErrorMessage returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


