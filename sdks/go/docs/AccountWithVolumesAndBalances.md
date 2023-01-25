# AccountWithVolumesAndBalances

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Address** | **string** |  | 
**Type** | Pointer to **string** |  | [optional] 
**Metadata** | Pointer to **map[string]interface{}** |  | [optional] 
**Volumes** | Pointer to **map[string]map[string]int64** |  | [optional] 
**Balances** | Pointer to **map[string]int64** |  | [optional] 

## Methods

### NewAccountWithVolumesAndBalances

`func NewAccountWithVolumesAndBalances(address string, ) *AccountWithVolumesAndBalances`

NewAccountWithVolumesAndBalances instantiates a new AccountWithVolumesAndBalances object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewAccountWithVolumesAndBalancesWithDefaults

`func NewAccountWithVolumesAndBalancesWithDefaults() *AccountWithVolumesAndBalances`

NewAccountWithVolumesAndBalancesWithDefaults instantiates a new AccountWithVolumesAndBalances object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetAddress

`func (o *AccountWithVolumesAndBalances) GetAddress() string`

GetAddress returns the Address field if non-nil, zero value otherwise.

### GetAddressOk

`func (o *AccountWithVolumesAndBalances) GetAddressOk() (*string, bool)`

GetAddressOk returns a tuple with the Address field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetAddress

`func (o *AccountWithVolumesAndBalances) SetAddress(v string)`

SetAddress sets Address field to given value.


### GetType

`func (o *AccountWithVolumesAndBalances) GetType() string`

GetType returns the Type field if non-nil, zero value otherwise.

### GetTypeOk

`func (o *AccountWithVolumesAndBalances) GetTypeOk() (*string, bool)`

GetTypeOk returns a tuple with the Type field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetType

`func (o *AccountWithVolumesAndBalances) SetType(v string)`

SetType sets Type field to given value.

### HasType

`func (o *AccountWithVolumesAndBalances) HasType() bool`

HasType returns a boolean if a field has been set.

### GetMetadata

`func (o *AccountWithVolumesAndBalances) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *AccountWithVolumesAndBalances) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *AccountWithVolumesAndBalances) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *AccountWithVolumesAndBalances) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetVolumes

`func (o *AccountWithVolumesAndBalances) GetVolumes() map[string]map[string]int64`

GetVolumes returns the Volumes field if non-nil, zero value otherwise.

### GetVolumesOk

`func (o *AccountWithVolumesAndBalances) GetVolumesOk() (*map[string]map[string]int64, bool)`

GetVolumesOk returns a tuple with the Volumes field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVolumes

`func (o *AccountWithVolumesAndBalances) SetVolumes(v map[string]map[string]int64)`

SetVolumes sets Volumes field to given value.

### HasVolumes

`func (o *AccountWithVolumesAndBalances) HasVolumes() bool`

HasVolumes returns a boolean if a field has been set.

### GetBalances

`func (o *AccountWithVolumesAndBalances) GetBalances() map[string]int64`

GetBalances returns the Balances field if non-nil, zero value otherwise.

### GetBalancesOk

`func (o *AccountWithVolumesAndBalances) GetBalancesOk() (*map[string]int64, bool)`

GetBalancesOk returns a tuple with the Balances field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetBalances

`func (o *AccountWithVolumesAndBalances) SetBalances(v map[string]int64)`

SetBalances sets Balances field to given value.

### HasBalances

`func (o *AccountWithVolumesAndBalances) HasBalances() bool`

HasBalances returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


