# PaymentAdjustment

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Status** | [**PaymentStatus**](PaymentStatus.md) |  | 
**Amount** | **int64** |  | 
**Date** | **time.Time** |  | 
**Raw** | **map[string]interface{}** |  | 
**Absolute** | **bool** |  | 

## Methods

### NewPaymentAdjustment

`func NewPaymentAdjustment(status PaymentStatus, amount int64, date time.Time, raw map[string]interface{}, absolute bool, ) *PaymentAdjustment`

NewPaymentAdjustment instantiates a new PaymentAdjustment object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewPaymentAdjustmentWithDefaults

`func NewPaymentAdjustmentWithDefaults() *PaymentAdjustment`

NewPaymentAdjustmentWithDefaults instantiates a new PaymentAdjustment object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetStatus

`func (o *PaymentAdjustment) GetStatus() PaymentStatus`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *PaymentAdjustment) GetStatusOk() (*PaymentStatus, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *PaymentAdjustment) SetStatus(v PaymentStatus)`

SetStatus sets Status field to given value.


### GetAmount

`func (o *PaymentAdjustment) GetAmount() int64`

GetAmount returns the Amount field if non-nil, zero value otherwise.

### GetAmountOk

`func (o *PaymentAdjustment) GetAmountOk() (*int64, bool)`

GetAmountOk returns a tuple with the Amount field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAmount

`func (o *PaymentAdjustment) SetAmount(v int64)`

SetAmount sets Amount field to given value.


### GetDate

`func (o *PaymentAdjustment) GetDate() time.Time`

GetDate returns the Date field if non-nil, zero value otherwise.

### GetDateOk

`func (o *PaymentAdjustment) GetDateOk() (*time.Time, bool)`

GetDateOk returns a tuple with the Date field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDate

`func (o *PaymentAdjustment) SetDate(v time.Time)`

SetDate sets Date field to given value.


### GetRaw

`func (o *PaymentAdjustment) GetRaw() map[string]interface{}`

GetRaw returns the Raw field if non-nil, zero value otherwise.

### GetRawOk

`func (o *PaymentAdjustment) GetRawOk() (*map[string]interface{}, bool)`

GetRawOk returns a tuple with the Raw field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetRaw

`func (o *PaymentAdjustment) SetRaw(v map[string]interface{})`

SetRaw sets Raw field to given value.


### GetAbsolute

`func (o *PaymentAdjustment) GetAbsolute() bool`

GetAbsolute returns the Absolute field if non-nil, zero value otherwise.

### GetAbsoluteOk

`func (o *PaymentAdjustment) GetAbsoluteOk() (*bool, bool)`

GetAbsoluteOk returns a tuple with the Absolute field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAbsolute

`func (o *PaymentAdjustment) SetAbsolute(v bool)`

SetAbsolute sets Absolute field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


