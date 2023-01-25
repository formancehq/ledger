# StackData

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** | Stack name |
**Tags** | **map[string]string** |  |
**Production** | **bool** |  |
**Metadata** | **map[string]string** |  |

## Methods

### NewStackData

`func NewStackData(name string, tags map[string]string, production bool, metadata map[string]string, ) *StackData`

NewStackData instantiates a new StackData object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewStackDataWithDefaults

`func NewStackDataWithDefaults() *StackData`

NewStackDataWithDefaults instantiates a new StackData object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *StackData) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *StackData) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *StackData) SetName(v string)`

SetName sets Name field to given value.


### GetTags

`func (o *StackData) GetTags() map[string]string`

GetTags returns the Tags field if non-nil, zero value otherwise.

### GetTagsOk

`func (o *StackData) GetTagsOk() (*map[string]string, bool)`

GetTagsOk returns a tuple with the Tags field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTags

`func (o *StackData) SetTags(v map[string]string)`

SetTags sets Tags field to given value.


### GetProduction

`func (o *StackData) GetProduction() bool`

GetProduction returns the Production field if non-nil, zero value otherwise.

### GetProductionOk

`func (o *StackData) GetProductionOk() (*bool, bool)`

GetProductionOk returns a tuple with the Production field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProduction

`func (o *StackData) SetProduction(v bool)`

SetProduction sets Production field to given value.


### GetMetadata

`func (o *StackData) GetMetadata() map[string]string`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *StackData) GetMetadataOk() (*map[string]string, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *StackData) SetMetadata(v map[string]string)`

SetMetadata sets Metadata field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
