# Monetary

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Asset** | **string** | The asset of the monetary value. | 
**Amount** | **int64** | The amount of the monetary value. | 

## Methods

### NewMonetary

`func NewMonetary(asset string, amount int64, ) *Monetary`

NewMonetary instantiates a new Monetary object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewMonetaryWithDefaults

`func NewMonetaryWithDefaults() *Monetary`

NewMonetaryWithDefaults instantiates a new Monetary object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetAsset

`func (o *Monetary) GetAsset() string`

GetAsset returns the Asset field if non-nil, zero value otherwise.

### GetAssetOk

`func (o *Monetary) GetAssetOk() (*string, bool)`

GetAssetOk returns a tuple with the Asset field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAsset

`func (o *Monetary) SetAsset(v string)`

SetAsset sets Asset field to given value.


### GetAmount

`func (o *Monetary) GetAmount() int64`

GetAmount returns the Amount field if non-nil, zero value otherwise.

### GetAmountOk

`func (o *Monetary) GetAmountOk() (*int64, bool)`

GetAmountOk returns a tuple with the Amount field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAmount

`func (o *Monetary) SetAmount(v int64)`

SetAmount sets Amount field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


