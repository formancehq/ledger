# ListConnectorTasks200ResponseInner

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Provider** | Pointer to **string** | The connector code | [optional]
**CreatedAt** | Pointer to **time.Time** | The date when the task was created | [optional]
**Status** | Pointer to **string** | The task status | [optional]
**Error** | Pointer to **string** | The error message if the task failed | [optional]
**State** | Pointer to **map[string]interface{}** | The task state | [optional]
**Descriptor** | Pointer to [**TaskDescriptorBankingCircleDescriptor**](TaskDescriptorBankingCircleDescriptor.md) |  | [optional]

## Methods

### NewListConnectorTasks200ResponseInner

`func NewListConnectorTasks200ResponseInner() *ListConnectorTasks200ResponseInner`

NewListConnectorTasks200ResponseInner instantiates a new ListConnectorTasks200ResponseInner object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewListConnectorTasks200ResponseInnerWithDefaults

`func NewListConnectorTasks200ResponseInnerWithDefaults() *ListConnectorTasks200ResponseInner`

NewListConnectorTasks200ResponseInnerWithDefaults instantiates a new ListConnectorTasks200ResponseInner object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetProvider

`func (o *ListConnectorTasks200ResponseInner) GetProvider() string`

GetProvider returns the Provider field if non-nil, zero value otherwise.

### GetProviderOk

`func (o *ListConnectorTasks200ResponseInner) GetProviderOk() (*string, bool)`

GetProviderOk returns a tuple with the Provider field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetProvider

`func (o *ListConnectorTasks200ResponseInner) SetProvider(v string)`

SetProvider sets Provider field to given value.

### HasProvider

`func (o *ListConnectorTasks200ResponseInner) HasProvider() bool`

HasProvider returns a boolean if a field has been set.

### GetCreatedAt

`func (o *ListConnectorTasks200ResponseInner) GetCreatedAt() time.Time`

GetCreatedAt returns the CreatedAt field if non-nil, zero value otherwise.

### GetCreatedAtOk

`func (o *ListConnectorTasks200ResponseInner) GetCreatedAtOk() (*time.Time, bool)`

GetCreatedAtOk returns a tuple with the CreatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedAt

`func (o *ListConnectorTasks200ResponseInner) SetCreatedAt(v time.Time)`

SetCreatedAt sets CreatedAt field to given value.

### HasCreatedAt

`func (o *ListConnectorTasks200ResponseInner) HasCreatedAt() bool`

HasCreatedAt returns a boolean if a field has been set.

### GetStatus

`func (o *ListConnectorTasks200ResponseInner) GetStatus() string`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *ListConnectorTasks200ResponseInner) GetStatusOk() (*string, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *ListConnectorTasks200ResponseInner) SetStatus(v string)`

SetStatus sets Status field to given value.

### HasStatus

`func (o *ListConnectorTasks200ResponseInner) HasStatus() bool`

HasStatus returns a boolean if a field has been set.

### GetError

`func (o *ListConnectorTasks200ResponseInner) GetError() string`

GetError returns the Error field if non-nil, zero value otherwise.

### GetErrorOk

`func (o *ListConnectorTasks200ResponseInner) GetErrorOk() (*string, bool)`

GetErrorOk returns a tuple with the Error field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetError

`func (o *ListConnectorTasks200ResponseInner) SetError(v string)`

SetError sets Error field to given value.

### HasError

`func (o *ListConnectorTasks200ResponseInner) HasError() bool`

HasError returns a boolean if a field has been set.

### GetState

`func (o *ListConnectorTasks200ResponseInner) GetState() map[string]interface{}`

GetState returns the State field if non-nil, zero value otherwise.

### GetStateOk

`func (o *ListConnectorTasks200ResponseInner) GetStateOk() (*map[string]interface{}, bool)`

GetStateOk returns a tuple with the State field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetState

`func (o *ListConnectorTasks200ResponseInner) SetState(v map[string]interface{})`

SetState sets State field to given value.

### HasState

`func (o *ListConnectorTasks200ResponseInner) HasState() bool`

HasState returns a boolean if a field has been set.

### GetDescriptor

`func (o *ListConnectorTasks200ResponseInner) GetDescriptor() TaskDescriptorBankingCircleDescriptor`

GetDescriptor returns the Descriptor field if non-nil, zero value otherwise.

### GetDescriptorOk

`func (o *ListConnectorTasks200ResponseInner) GetDescriptorOk() (*TaskDescriptorBankingCircleDescriptor, bool)`

GetDescriptorOk returns a tuple with the Descriptor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescriptor

`func (o *ListConnectorTasks200ResponseInner) SetDescriptor(v TaskDescriptorBankingCircleDescriptor)`

SetDescriptor sets Descriptor field to given value.

### HasDescriptor

`func (o *ListConnectorTasks200ResponseInner) HasDescriptor() bool`

HasDescriptor returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
