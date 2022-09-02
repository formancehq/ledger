# CreateTransactions400Response

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ErrorCode** | **string** |  | 
**ErrorMessage** | Pointer to **string** |  | [optional] 

## Methods

### NewCreateTransactions400Response

`func NewCreateTransactions400Response(errorCode string, ) *CreateTransactions400Response`

NewCreateTransactions400Response instantiates a new CreateTransactions400Response object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewCreateTransactions400ResponseWithDefaults

`func NewCreateTransactions400ResponseWithDefaults() *CreateTransactions400Response`

NewCreateTransactions400ResponseWithDefaults instantiates a new CreateTransactions400Response object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetErrorCode

`func (o *CreateTransactions400Response) GetErrorCode() string`

GetErrorCode returns the ErrorCode field if non-nil, zero value otherwise.

### GetErrorCodeOk

`func (o *CreateTransactions400Response) GetErrorCodeOk() (*string, bool)`

GetErrorCodeOk returns a tuple with the ErrorCode field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorCode

`func (o *CreateTransactions400Response) SetErrorCode(v string)`

SetErrorCode sets ErrorCode field to given value.


### GetErrorMessage

`func (o *CreateTransactions400Response) GetErrorMessage() string`

GetErrorMessage returns the ErrorMessage field if non-nil, zero value otherwise.

### GetErrorMessageOk

`func (o *CreateTransactions400Response) GetErrorMessageOk() (*string, bool)`

GetErrorMessageOk returns a tuple with the ErrorMessage field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorMessage

`func (o *CreateTransactions400Response) SetErrorMessage(v string)`

SetErrorMessage sets ErrorMessage field to given value.

### HasErrorMessage

`func (o *CreateTransactions400Response) HasErrorMessage() bool`

HasErrorMessage returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


