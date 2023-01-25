# Secret

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** |  | 
**Metadata** | Pointer to **map[string]interface{}** |  | [optional] 
**Id** | **string** |  | 
**LastDigits** | **string** |  | 
**Clear** | **string** |  | 

## Methods

### NewSecret

`func NewSecret(name string, id string, lastDigits string, clear string, ) *Secret`

NewSecret instantiates a new Secret object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewSecretWithDefaults

`func NewSecretWithDefaults() *Secret`

NewSecretWithDefaults instantiates a new Secret object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *Secret) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *Secret) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *Secret) SetName(v string)`

SetName sets Name field to given value.


### GetMetadata

`func (o *Secret) GetMetadata() map[string]interface{}`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Secret) GetMetadataOk() (*map[string]interface{}, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Secret) SetMetadata(v map[string]interface{})`

SetMetadata sets Metadata field to given value.

### HasMetadata

`func (o *Secret) HasMetadata() bool`

HasMetadata returns a boolean if a field has been set.

### GetId

`func (o *Secret) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *Secret) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *Secret) SetId(v string)`

SetId sets Id field to given value.


### GetLastDigits

`func (o *Secret) GetLastDigits() string`

GetLastDigits returns the LastDigits field if non-nil, zero value otherwise.

### GetLastDigitsOk

`func (o *Secret) GetLastDigitsOk() (*string, bool)`

GetLastDigitsOk returns a tuple with the LastDigits field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetLastDigits

`func (o *Secret) SetLastDigits(v string)`

SetLastDigits sets LastDigits field to given value.


### GetClear

`func (o *Secret) GetClear() string`

GetClear returns the Clear field if non-nil, zero value otherwise.

### GetClearOk

`func (o *Secret) GetClearOk() (*string, bool)`

GetClearOk returns a tuple with the Clear field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetClear

`func (o *Secret) SetClear(v string)`

SetClear sets Clear field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


