# BalanceWithAssets

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Assets** | **map[string]float32** |  | 

## Methods

### NewBalanceWithAssets

`func NewBalanceWithAssets(name string, assets map[string]float32, ) *BalanceWithAssets`

NewBalanceWithAssets instantiates a new BalanceWithAssets object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewBalanceWithAssetsWithDefaults

`func NewBalanceWithAssetsWithDefaults() *BalanceWithAssets`

NewBalanceWithAssetsWithDefaults instantiates a new BalanceWithAssets object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *BalanceWithAssets) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *BalanceWithAssets) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *BalanceWithAssets) SetName(v string)`

SetName sets Name field to given value.


### GetAssets

`func (o *BalanceWithAssets) GetAssets() map[string]float32`

GetAssets returns the Assets field if non-nil, zero value otherwise.

### GetAssetsOk

`func (o *BalanceWithAssets) GetAssetsOk() (*map[string]float32, bool)`

GetAssetsOk returns a tuple with the Assets field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAssets

`func (o *BalanceWithAssets) SetAssets(v map[string]float32)`

SetAssets sets Assets field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


