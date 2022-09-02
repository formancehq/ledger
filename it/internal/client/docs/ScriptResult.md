# ScriptResult

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Details** | Pointer to **string** |  | [optional] 
**ErrorCode** | Pointer to **string** |  | [optional] 
**ErrorMessage** | Pointer to **string** |  | [optional] 
**Transaction** | Pointer to [**Transaction**](Transaction.md) |  | [optional] 

## Methods

### NewScriptResult

`func NewScriptResult() *ScriptResult`

NewScriptResult instantiates a new ScriptResult object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewScriptResultWithDefaults

`func NewScriptResultWithDefaults() *ScriptResult`

NewScriptResultWithDefaults instantiates a new ScriptResult object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetDetails

`func (o *ScriptResult) GetDetails() string`

GetDetails returns the Details field if non-nil, zero value otherwise.

### GetDetailsOk

`func (o *ScriptResult) GetDetailsOk() (*string, bool)`

GetDetailsOk returns a tuple with the Details field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDetails

`func (o *ScriptResult) SetDetails(v string)`

SetDetails sets Details field to given value.

### HasDetails

`func (o *ScriptResult) HasDetails() bool`

HasDetails returns a boolean if a field has been set.

### GetErrorCode

`func (o *ScriptResult) GetErrorCode() string`

GetErrorCode returns the ErrorCode field if non-nil, zero value otherwise.

### GetErrorCodeOk

`func (o *ScriptResult) GetErrorCodeOk() (*string, bool)`

GetErrorCodeOk returns a tuple with the ErrorCode field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorCode

`func (o *ScriptResult) SetErrorCode(v string)`

SetErrorCode sets ErrorCode field to given value.

### HasErrorCode

`func (o *ScriptResult) HasErrorCode() bool`

HasErrorCode returns a boolean if a field has been set.

### GetErrorMessage

`func (o *ScriptResult) GetErrorMessage() string`

GetErrorMessage returns the ErrorMessage field if non-nil, zero value otherwise.

### GetErrorMessageOk

`func (o *ScriptResult) GetErrorMessageOk() (*string, bool)`

GetErrorMessageOk returns a tuple with the ErrorMessage field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetErrorMessage

`func (o *ScriptResult) SetErrorMessage(v string)`

SetErrorMessage sets ErrorMessage field to given value.

### HasErrorMessage

`func (o *ScriptResult) HasErrorMessage() bool`

HasErrorMessage returns a boolean if a field has been set.

### GetTransaction

`func (o *ScriptResult) GetTransaction() Transaction`

GetTransaction returns the Transaction field if non-nil, zero value otherwise.

### GetTransactionOk

`func (o *ScriptResult) GetTransactionOk() (*Transaction, bool)`

GetTransactionOk returns a tuple with the Transaction field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTransaction

`func (o *ScriptResult) SetTransaction(v Transaction)`

SetTransaction sets Transaction field to given value.

### HasTransaction

`func (o *ScriptResult) HasTransaction() bool`

HasTransaction returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


