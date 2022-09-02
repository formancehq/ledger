# ListAccounts400Response

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ErrorCode** | **string** |  | 
**ErrorMessage** | Pointer to **string** |  | [optional] 

## Methods

### NewListAccounts400Response

`func NewListAccounts400Response(errorCode string, ) *ListAccounts400Response`

NewListAccounts400Response instantiates a new ListAccounts400Response object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewListAccounts400ResponseWithDefaults

`func NewListAccounts400ResponseWithDefaults() *ListAccounts400Response`

NewListAccounts400ResponseWithDefaults instantiates a new ListAccounts400Response object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetErrorCode

`func (o *ListAccounts400Response) GetErrorCode() string`

GetErrorCode returns the ErrorCode field if non-nil, zero value otherwise.

### GetErrorCodeOk

`func (o *ListAccounts400Response) GetErrorCodeOk() (*string, bool)`

GetErrorCodeOk returns a tuple with the ErrorCode field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorCode

`func (o *ListAccounts400Response) SetErrorCode(v string)`

SetErrorCode sets ErrorCode field to given value.


### GetErrorMessage

`func (o *ListAccounts400Response) GetErrorMessage() string`

GetErrorMessage returns the ErrorMessage field if non-nil, zero value otherwise.

### GetErrorMessageOk

`func (o *ListAccounts400Response) GetErrorMessageOk() (*string, bool)`

GetErrorMessageOk returns a tuple with the ErrorMessage field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorMessage

`func (o *ListAccounts400Response) SetErrorMessage(v string)`

SetErrorMessage sets ErrorMessage field to given value.

### HasErrorMessage

`func (o *ListAccounts400Response) HasErrorMessage() bool`

HasErrorMessage returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


