# TaskDescriptorModulr

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Provider** | Pointer to **string** | The connector code | [optional]
**CreatedAt** | Pointer to **time.Time** | The date when the task was created | [optional]
**Status** | Pointer to **string** | The task status | [optional]
**Error** | Pointer to **string** | The error message if the task failed | [optional]
**State** | Pointer to **map[string]interface{}** | The task state | [optional]
**Descriptor** | Pointer to [**TaskDescriptorModulrDescriptor**](TaskDescriptorModulrDescriptor.md) |  | [optional]

## Methods

### NewTaskDescriptorModulr

`func NewTaskDescriptorModulr() *TaskDescriptorModulr`

NewTaskDescriptorModulr instantiates a new TaskDescriptorModulr object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTaskDescriptorModulrWithDefaults

`func NewTaskDescriptorModulrWithDefaults() *TaskDescriptorModulr`

NewTaskDescriptorModulrWithDefaults instantiates a new TaskDescriptorModulr object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetProvider

`func (o *TaskDescriptorModulr) GetProvider() string`

GetProvider returns the Provider field if non-nil, zero value otherwise.

### GetProviderOk

`func (o *TaskDescriptorModulr) GetProviderOk() (*string, bool)`

GetProviderOk returns a tuple with the Provider field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProvider

`func (o *TaskDescriptorModulr) SetProvider(v string)`

SetProvider sets Provider field to given value.

### HasProvider

`func (o *TaskDescriptorModulr) HasProvider() bool`

HasProvider returns a boolean if a field has been set.

### GetCreatedAt

`func (o *TaskDescriptorModulr) GetCreatedAt() time.Time`

GetCreatedAt returns the CreatedAt field if non-nil, zero value otherwise.

### GetCreatedAtOk

`func (o *TaskDescriptorModulr) GetCreatedAtOk() (*time.Time, bool)`

GetCreatedAtOk returns a tuple with the CreatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedAt

`func (o *TaskDescriptorModulr) SetCreatedAt(v time.Time)`

SetCreatedAt sets CreatedAt field to given value.

### HasCreatedAt

`func (o *TaskDescriptorModulr) HasCreatedAt() bool`

HasCreatedAt returns a boolean if a field has been set.

### GetStatus

`func (o *TaskDescriptorModulr) GetStatus() string`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *TaskDescriptorModulr) GetStatusOk() (*string, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *TaskDescriptorModulr) SetStatus(v string)`

SetStatus sets Status field to given value.

### HasStatus

`func (o *TaskDescriptorModulr) HasStatus() bool`

HasStatus returns a boolean if a field has been set.

### GetError

`func (o *TaskDescriptorModulr) GetError() string`

GetError returns the Error field if non-nil, zero value otherwise.

### GetErrorOk

`func (o *TaskDescriptorModulr) GetErrorOk() (*string, bool)`

GetErrorOk returns a tuple with the Error field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetError

`func (o *TaskDescriptorModulr) SetError(v string)`

SetError sets Error field to given value.

### HasError

`func (o *TaskDescriptorModulr) HasError() bool`

HasError returns a boolean if a field has been set.

### GetState

`func (o *TaskDescriptorModulr) GetState() map[string]interface{}`

GetState returns the State field if non-nil, zero value otherwise.

### GetStateOk

`func (o *TaskDescriptorModulr) GetStateOk() (*map[string]interface{}, bool)`

GetStateOk returns a tuple with the State field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetState

`func (o *TaskDescriptorModulr) SetState(v map[string]interface{})`

SetState sets State field to given value.

### HasState

`func (o *TaskDescriptorModulr) HasState() bool`

HasState returns a boolean if a field has been set.

### GetDescriptor

`func (o *TaskDescriptorModulr) GetDescriptor() TaskDescriptorModulrDescriptor`

GetDescriptor returns the Descriptor field if non-nil, zero value otherwise.

### GetDescriptorOk

`func (o *TaskDescriptorModulr) GetDescriptorOk() (*TaskDescriptorModulrDescriptor, bool)`

GetDescriptorOk returns a tuple with the Descriptor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescriptor

`func (o *TaskDescriptorModulr) SetDescriptor(v TaskDescriptorModulrDescriptor)`

SetDescriptor sets Descriptor field to given value.

### HasDescriptor

`func (o *TaskDescriptorModulr) HasDescriptor() bool`

HasDescriptor returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
