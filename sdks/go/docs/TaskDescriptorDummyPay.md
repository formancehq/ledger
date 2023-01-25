# TaskDescriptorDummyPay

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Provider** | Pointer to **string** | The connector code | [optional]
**CreatedAt** | Pointer to **time.Time** | The date when the task was created | [optional]
**Status** | Pointer to **string** | The task status | [optional]
**Error** | Pointer to **string** | The error message if the task failed | [optional]
**State** | Pointer to **map[string]interface{}** | The task state | [optional]
**Descriptor** | Pointer to [**TaskDescriptorDummyPayDescriptor**](TaskDescriptorDummyPayDescriptor.md) |  | [optional]

## Methods

### NewTaskDescriptorDummyPay

`func NewTaskDescriptorDummyPay() *TaskDescriptorDummyPay`

NewTaskDescriptorDummyPay instantiates a new TaskDescriptorDummyPay object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTaskDescriptorDummyPayWithDefaults

`func NewTaskDescriptorDummyPayWithDefaults() *TaskDescriptorDummyPay`

NewTaskDescriptorDummyPayWithDefaults instantiates a new TaskDescriptorDummyPay object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetProvider

`func (o *TaskDescriptorDummyPay) GetProvider() string`

GetProvider returns the Provider field if non-nil, zero value otherwise.

### GetProviderOk

`func (o *TaskDescriptorDummyPay) GetProviderOk() (*string, bool)`

GetProviderOk returns a tuple with the Provider field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProvider

`func (o *TaskDescriptorDummyPay) SetProvider(v string)`

SetProvider sets Provider field to given value.

### HasProvider

`func (o *TaskDescriptorDummyPay) HasProvider() bool`

HasProvider returns a boolean if a field has been set.

### GetCreatedAt

`func (o *TaskDescriptorDummyPay) GetCreatedAt() time.Time`

GetCreatedAt returns the CreatedAt field if non-nil, zero value otherwise.

### GetCreatedAtOk

`func (o *TaskDescriptorDummyPay) GetCreatedAtOk() (*time.Time, bool)`

GetCreatedAtOk returns a tuple with the CreatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedAt

`func (o *TaskDescriptorDummyPay) SetCreatedAt(v time.Time)`

SetCreatedAt sets CreatedAt field to given value.

### HasCreatedAt

`func (o *TaskDescriptorDummyPay) HasCreatedAt() bool`

HasCreatedAt returns a boolean if a field has been set.

### GetStatus

`func (o *TaskDescriptorDummyPay) GetStatus() string`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *TaskDescriptorDummyPay) GetStatusOk() (*string, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *TaskDescriptorDummyPay) SetStatus(v string)`

SetStatus sets Status field to given value.

### HasStatus

`func (o *TaskDescriptorDummyPay) HasStatus() bool`

HasStatus returns a boolean if a field has been set.

### GetError

`func (o *TaskDescriptorDummyPay) GetError() string`

GetError returns the Error field if non-nil, zero value otherwise.

### GetErrorOk

`func (o *TaskDescriptorDummyPay) GetErrorOk() (*string, bool)`

GetErrorOk returns a tuple with the Error field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetError

`func (o *TaskDescriptorDummyPay) SetError(v string)`

SetError sets Error field to given value.

### HasError

`func (o *TaskDescriptorDummyPay) HasError() bool`

HasError returns a boolean if a field has been set.

### GetState

`func (o *TaskDescriptorDummyPay) GetState() map[string]interface{}`

GetState returns the State field if non-nil, zero value otherwise.

### GetStateOk

`func (o *TaskDescriptorDummyPay) GetStateOk() (*map[string]interface{}, bool)`

GetStateOk returns a tuple with the State field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetState

`func (o *TaskDescriptorDummyPay) SetState(v map[string]interface{})`

SetState sets State field to given value.

### HasState

`func (o *TaskDescriptorDummyPay) HasState() bool`

HasState returns a boolean if a field has been set.

### GetDescriptor

`func (o *TaskDescriptorDummyPay) GetDescriptor() TaskDescriptorDummyPayDescriptor`

GetDescriptor returns the Descriptor field if non-nil, zero value otherwise.

### GetDescriptorOk

`func (o *TaskDescriptorDummyPay) GetDescriptorOk() (*TaskDescriptorDummyPayDescriptor, bool)`

GetDescriptorOk returns a tuple with the Descriptor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescriptor

`func (o *TaskDescriptorDummyPay) SetDescriptor(v TaskDescriptorDummyPayDescriptor)`

SetDescriptor sets Descriptor field to given value.

### HasDescriptor

`func (o *TaskDescriptorDummyPay) HasDescriptor() bool`

HasDescriptor returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
