# Stack

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** | Stack name |
**Tags** | **map[string]string** |  |
**Production** | **bool** |  |
**Metadata** | **map[string]string** |  |
**Id** | **string** | Stack ID |
**OrganizationId** | **string** | Organization ID |
**Uri** | **string** | Base stack uri |
**BoundRegion** | Pointer to [**Region**](Region.md) |  | [optional]

## Methods

### NewStack

`func NewStack(name string, tags map[string]string, production bool, metadata map[string]string, id string, organizationId string, uri string, ) *Stack`

NewStack instantiates a new Stack object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewStackWithDefaults

`func NewStackWithDefaults() *Stack`

NewStackWithDefaults instantiates a new Stack object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *Stack) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *Stack) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *Stack) SetName(v string)`

SetName sets Name field to given value.


### GetTags

`func (o *Stack) GetTags() map[string]string`

GetTags returns the Tags field if non-nil, zero value otherwise.

### GetTagsOk

`func (o *Stack) GetTagsOk() (*map[string]string, bool)`

GetTagsOk returns a tuple with the Tags field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTags

`func (o *Stack) SetTags(v map[string]string)`

SetTags sets Tags field to given value.


### GetProduction

`func (o *Stack) GetProduction() bool`

GetProduction returns the Production field if non-nil, zero value otherwise.

### GetProductionOk

`func (o *Stack) GetProductionOk() (*bool, bool)`

GetProductionOk returns a tuple with the Production field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProduction

`func (o *Stack) SetProduction(v bool)`

SetProduction sets Production field to given value.


### GetMetadata

`func (o *Stack) GetMetadata() map[string]string`

GetMetadata returns the Metadata field if non-nil, zero value otherwise.

### GetMetadataOk

`func (o *Stack) GetMetadataOk() (*map[string]string, bool)`

GetMetadataOk returns a tuple with the Metadata field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetMetadata

`func (o *Stack) SetMetadata(v map[string]string)`

SetMetadata sets Metadata field to given value.


### GetId

`func (o *Stack) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *Stack) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *Stack) SetId(v string)`

SetId sets Id field to given value.


### GetOrganizationId

`func (o *Stack) GetOrganizationId() string`

GetOrganizationId returns the OrganizationId field if non-nil, zero value otherwise.

### GetOrganizationIdOk

`func (o *Stack) GetOrganizationIdOk() (*string, bool)`

GetOrganizationIdOk returns a tuple with the OrganizationId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOrganizationId

`func (o *Stack) SetOrganizationId(v string)`

SetOrganizationId sets OrganizationId field to given value.


### GetUri

`func (o *Stack) GetUri() string`

GetUri returns the Uri field if non-nil, zero value otherwise.

### GetUriOk

`func (o *Stack) GetUriOk() (*string, bool)`

GetUriOk returns a tuple with the Uri field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUri

`func (o *Stack) SetUri(v string)`

SetUri sets Uri field to given value.


### GetBoundRegion

`func (o *Stack) GetBoundRegion() Region`

GetBoundRegion returns the BoundRegion field if non-nil, zero value otherwise.

### GetBoundRegionOk

`func (o *Stack) GetBoundRegionOk() (*Region, bool)`

GetBoundRegionOk returns a tuple with the BoundRegion field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetBoundRegion

`func (o *Stack) SetBoundRegion(v Region)`

SetBoundRegion sets BoundRegion field to given value.

### HasBoundRegion

`func (o *Stack) HasBoundRegion() bool`

HasBoundRegion returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
