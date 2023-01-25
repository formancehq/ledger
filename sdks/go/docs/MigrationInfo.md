# MigrationInfo

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Version** | Pointer to **int64** |  | [optional] 
**Name** | Pointer to **string** |  | [optional] 
**Date** | Pointer to **time.Time** |  | [optional] 
**State** | Pointer to **string** |  | [optional] 

## Methods

### NewMigrationInfo

`func NewMigrationInfo() *MigrationInfo`

NewMigrationInfo instantiates a new MigrationInfo object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewMigrationInfoWithDefaults

`func NewMigrationInfoWithDefaults() *MigrationInfo`

NewMigrationInfoWithDefaults instantiates a new MigrationInfo object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetVersion

`func (o *MigrationInfo) GetVersion() int64`

GetVersion returns the Version field if non-nil, zero value otherwise.

### GetVersionOk

`func (o *MigrationInfo) GetVersionOk() (*int64, bool)`

GetVersionOk returns a tuple with the Version field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetVersion

`func (o *MigrationInfo) SetVersion(v int64)`

SetVersion sets Version field to given value.

### HasVersion

`func (o *MigrationInfo) HasVersion() bool`

HasVersion returns a boolean if a field has been set.

### GetName

`func (o *MigrationInfo) GetName() string`

GetName returns the Name field if non-nil, zero value otherwise.

### GetNameOk

`func (o *MigrationInfo) GetNameOk() (*string, bool)`

GetNameOk returns a tuple with the Name field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetName

`func (o *MigrationInfo) SetName(v string)`

SetName sets Name field to given value.

### HasName

`func (o *MigrationInfo) HasName() bool`

HasName returns a boolean if a field has been set.

### GetDate

`func (o *MigrationInfo) GetDate() time.Time`

GetDate returns the Date field if non-nil, zero value otherwise.

### GetDateOk

`func (o *MigrationInfo) GetDateOk() (*time.Time, bool)`

GetDateOk returns a tuple with the Date field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDate

`func (o *MigrationInfo) SetDate(v time.Time)`

SetDate sets Date field to given value.

### HasDate

`func (o *MigrationInfo) HasDate() bool`

HasDate returns a boolean if a field has been set.

### GetState

`func (o *MigrationInfo) GetState() string`

GetState returns the State field if non-nil, zero value otherwise.

### GetStateOk

`func (o *MigrationInfo) GetStateOk() (*string, bool)`

GetStateOk returns a tuple with the State field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetState

`func (o *MigrationInfo) SetState(v string)`

SetState sets State field to given value.

### HasState

`func (o *MigrationInfo) HasState() bool`

HasState returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


