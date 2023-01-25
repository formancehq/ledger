# StackAllOf

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **string** | Stack ID |
**OrganizationId** | **string** | Organization ID |
**Uri** | **string** | Base stack uri |
**BoundRegion** | Pointer to [**Region**](Region.md) |  | [optional]

## Methods

### NewStackAllOf

`func NewStackAllOf(id string, organizationId string, uri string, ) *StackAllOf`

NewStackAllOf instantiates a new StackAllOf object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewStackAllOfWithDefaults

`func NewStackAllOfWithDefaults() *StackAllOf`

NewStackAllOfWithDefaults instantiates a new StackAllOf object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *StackAllOf) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *StackAllOf) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *StackAllOf) SetId(v string)`

SetId sets Id field to given value.


### GetOrganizationId

`func (o *StackAllOf) GetOrganizationId() string`

GetOrganizationId returns the OrganizationId field if non-nil, zero value otherwise.

### GetOrganizationIdOk

`func (o *StackAllOf) GetOrganizationIdOk() (*string, bool)`

GetOrganizationIdOk returns a tuple with the OrganizationId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOrganizationId

`func (o *StackAllOf) SetOrganizationId(v string)`

SetOrganizationId sets OrganizationId field to given value.


### GetUri

`func (o *StackAllOf) GetUri() string`

GetUri returns the Uri field if non-nil, zero value otherwise.

### GetUriOk

`func (o *StackAllOf) GetUriOk() (*string, bool)`

GetUriOk returns a tuple with the Uri field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUri

`func (o *StackAllOf) SetUri(v string)`

SetUri sets Uri field to given value.


### GetBoundRegion

`func (o *StackAllOf) GetBoundRegion() Region`

GetBoundRegion returns the BoundRegion field if non-nil, zero value otherwise.

### GetBoundRegionOk

`func (o *StackAllOf) GetBoundRegionOk() (*Region, bool)`

GetBoundRegionOk returns a tuple with the BoundRegion field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetBoundRegion

`func (o *StackAllOf) SetBoundRegion(v Region)`

SetBoundRegion sets BoundRegion field to given value.

### HasBoundRegion

`func (o *StackAllOf) HasBoundRegion() bool`

HasBoundRegion returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
