# ListOrganizationExpandedResponseDataInner

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Name** | **string** | Organization name |
**Id** | **string** | Organization ID |
**OwnerId** | **string** | Owner ID |
**TotalStacks** | Pointer to **int32** |  | [optional]
**TotalUsers** | Pointer to **int32** |  | [optional]

## Methods

### NewListOrganizationExpandedResponseDataInner

`func NewListOrganizationExpandedResponseDataInner(name string, id string, ownerId string, ) *ListOrganizationExpandedResponseDataInner`

NewListOrganizationExpandedResponseDataInner instantiates a new ListOrganizationExpandedResponseDataInner object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewListOrganizationExpandedResponseDataInnerWithDefaults

`func NewListOrganizationExpandedResponseDataInnerWithDefaults() *ListOrganizationExpandedResponseDataInner`

NewListOrganizationExpandedResponseDataInnerWithDefaults instantiates a new ListOrganizationExpandedResponseDataInner object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetName

`func (o *ListOrganizationExpandedResponseDataInner) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *ListOrganizationExpandedResponseDataInner) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *ListOrganizationExpandedResponseDataInner) SetName(v string)`

SetName sets Name field to given value.


### GetId

`func (o *ListOrganizationExpandedResponseDataInner) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *ListOrganizationExpandedResponseDataInner) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *ListOrganizationExpandedResponseDataInner) SetId(v string)`

SetId sets Id field to given value.


### GetOwnerId

`func (o *ListOrganizationExpandedResponseDataInner) GetOwnerId() string`

GetOwnerId returns the OwnerId field if non-nil, zero value otherwise.

### GetOwnerIdOk

`func (o *ListOrganizationExpandedResponseDataInner) GetOwnerIdOk() (*string, bool)`

GetOwnerIdOk returns a tuple with the OwnerId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOwnerId

`func (o *ListOrganizationExpandedResponseDataInner) SetOwnerId(v string)`

SetOwnerId sets OwnerId field to given value.


### GetTotalStacks

`func (o *ListOrganizationExpandedResponseDataInner) GetTotalStacks() int32`

GetTotalStacks returns the TotalStacks field if non-nil, zero value otherwise.

### GetTotalStacksOk

`func (o *ListOrganizationExpandedResponseDataInner) GetTotalStacksOk() (*int32, bool)`

GetTotalStacksOk returns a tuple with the TotalStacks field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTotalStacks

`func (o *ListOrganizationExpandedResponseDataInner) SetTotalStacks(v int32)`

SetTotalStacks sets TotalStacks field to given value.

### HasTotalStacks

`func (o *ListOrganizationExpandedResponseDataInner) HasTotalStacks() bool`

HasTotalStacks returns a boolean if a field has been set.

### GetTotalUsers

`func (o *ListOrganizationExpandedResponseDataInner) GetTotalUsers() int32`

GetTotalUsers returns the TotalUsers field if non-nil, zero value otherwise.

### GetTotalUsersOk

`func (o *ListOrganizationExpandedResponseDataInner) GetTotalUsersOk() (*int32, bool)`

GetTotalUsersOk returns a tuple with the TotalUsers field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTotalUsers

`func (o *ListOrganizationExpandedResponseDataInner) SetTotalUsers(v int32)`

SetTotalUsers sets TotalUsers field to given value.

### HasTotalUsers

`func (o *ListOrganizationExpandedResponseDataInner) HasTotalUsers() bool`

HasTotalUsers returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
