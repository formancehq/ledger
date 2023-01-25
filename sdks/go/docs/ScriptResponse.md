# ScriptResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**ErrorCode** | Pointer to [**ErrorsEnum**](ErrorsEnum.md) |  | [optional] 
**ErrorMessage** | Pointer to **string** |  | [optional] 
**Details** | Pointer to **string** |  | [optional] 
**Transaction** | Pointer to [**Transaction**](Transaction.md) |  | [optional] 

## Methods

### NewScriptResponse

`func NewScriptResponse() *ScriptResponse`

NewScriptResponse instantiates a new ScriptResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewScriptResponseWithDefaults

`func NewScriptResponseWithDefaults() *ScriptResponse`

NewScriptResponseWithDefaults instantiates a new ScriptResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetErrorCode

`func (o *ScriptResponse) GetErrorCode() ErrorsEnum`

GetErrorCode returns the ErrorCode field if non-nil, zero value otherwise.

### GetErrorCodeOk

`func (o *ScriptResponse) GetErrorCodeOk() (*ErrorsEnum, bool)`

GetErrorCodeOk returns a tuple with the ErrorCode field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorCode

`func (o *ScriptResponse) SetErrorCode(v ErrorsEnum)`

SetErrorCode sets ErrorCode field to given value.

### HasErrorCode

`func (o *ScriptResponse) HasErrorCode() bool`

HasErrorCode returns a boolean if a field has been set.

### GetErrorMessage

`func (o *ScriptResponse) GetErrorMessage() string`

GetErrorMessage returns the ErrorMessage field if non-nil, zero value otherwise.

### GetErrorMessageOk

`func (o *ScriptResponse) GetErrorMessageOk() (*string, bool)`

GetErrorMessageOk returns a tuple with the ErrorMessage field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorMessage

`func (o *ScriptResponse) SetErrorMessage(v string)`

SetErrorMessage sets ErrorMessage field to given value.

### HasErrorMessage

`func (o *ScriptResponse) HasErrorMessage() bool`

HasErrorMessage returns a boolean if a field has been set.

### GetDetails

`func (o *ScriptResponse) GetDetails() string`

GetDetails returns the Details field if non-nil, zero value otherwise.

### GetDetailsOk

`func (o *ScriptResponse) GetDetailsOk() (*string, bool)`

GetDetailsOk returns a tuple with the Details field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDetails

`func (o *ScriptResponse) SetDetails(v string)`

SetDetails sets Details field to given value.

### HasDetails

`func (o *ScriptResponse) HasDetails() bool`

HasDetails returns a boolean if a field has been set.

### GetTransaction

`func (o *ScriptResponse) GetTransaction() Transaction`

GetTransaction returns the Transaction field if non-nil, zero value otherwise.

### GetTransactionOk

`func (o *ScriptResponse) GetTransactionOk() (*Transaction, bool)`

GetTransactionOk returns a tuple with the Transaction field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTransaction

`func (o *ScriptResponse) SetTransaction(v Transaction)`

SetTransaction sets Transaction field to given value.

### HasTransaction

`func (o *ScriptResponse) HasTransaction() bool`

HasTransaction returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


