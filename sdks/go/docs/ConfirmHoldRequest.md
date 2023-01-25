# ConfirmHoldRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Amount** | Pointer to **int64** | Define the amount to transfer. | [optional] 
**Final** | Pointer to **bool** | Define a final confirmation. Remaining funds will be returned to the wallet. | [optional] 

## Methods

### NewConfirmHoldRequest

`func NewConfirmHoldRequest() *ConfirmHoldRequest`

NewConfirmHoldRequest instantiates a new ConfirmHoldRequest object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewConfirmHoldRequestWithDefaults

`func NewConfirmHoldRequestWithDefaults() *ConfirmHoldRequest`

NewConfirmHoldRequestWithDefaults instantiates a new ConfirmHoldRequest object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetAmount

`func (o *ConfirmHoldRequest) GetAmount() int64`

GetAmount returns the Amount field if non-nil, zero value otherwise.

### GetAmountOk

`func (o *ConfirmHoldRequest) GetAmountOk() (*int64, bool)`

GetAmountOk returns a tuple with the Amount field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAmount

`func (o *ConfirmHoldRequest) SetAmount(v int64)`

SetAmount sets Amount field to given value.

### HasAmount

`func (o *ConfirmHoldRequest) HasAmount() bool`

HasAmount returns a boolean if a field has been set.

### GetFinal

`func (o *ConfirmHoldRequest) GetFinal() bool`

GetFinal returns the Final field if non-nil, zero value otherwise.

### GetFinalOk

`func (o *ConfirmHoldRequest) GetFinalOk() (*bool, bool)`

GetFinalOk returns a tuple with the Final field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetFinal

`func (o *ConfirmHoldRequest) SetFinal(v bool)`

SetFinal sets Final field to given value.

### HasFinal

`func (o *ConfirmHoldRequest) HasFinal() bool`

HasFinal returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


