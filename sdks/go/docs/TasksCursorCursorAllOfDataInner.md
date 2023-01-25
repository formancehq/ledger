# TasksCursorCursorAllOfDataInner

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Id** | **string** |  | 
**ConnectorId** | **string** |  | 
**CreatedAt** | **time.Time** |  | 
**UpdatedAt** | **time.Time** |  | 
**Status** | [**PaymentStatus**](PaymentStatus.md) |  | 
**State** | **map[string]interface{}** |  | 
**Error** | Pointer to **string** |  | [optional] 
**Descriptor** | [**TaskBankingCircleAllOfDescriptor**](TaskBankingCircleAllOfDescriptor.md) |  | 

## Methods

### NewTasksCursorCursorAllOfDataInner

`func NewTasksCursorCursorAllOfDataInner(id string, connectorId string, createdAt time.Time, updatedAt time.Time, status PaymentStatus, state map[string]interface{}, descriptor TaskBankingCircleAllOfDescriptor, ) *TasksCursorCursorAllOfDataInner`

NewTasksCursorCursorAllOfDataInner instantiates a new TasksCursorCursorAllOfDataInner object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewTasksCursorCursorAllOfDataInnerWithDefaults

`func NewTasksCursorCursorAllOfDataInnerWithDefaults() *TasksCursorCursorAllOfDataInner`

NewTasksCursorCursorAllOfDataInnerWithDefaults instantiates a new TasksCursorCursorAllOfDataInner object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetId

`func (o *TasksCursorCursorAllOfDataInner) GetId() string`

GetId returns the Id field if non-nil, zero value otherwise.

### GetIdOk

`func (o *TasksCursorCursorAllOfDataInner) GetIdOk() (*string, bool)`

GetIdOk returns a tuple with the Id field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetId

`func (o *TasksCursorCursorAllOfDataInner) SetId(v string)`

SetId sets Id field to given value.


### GetConnectorId

`func (o *TasksCursorCursorAllOfDataInner) GetConnectorId() string`

GetConnectorId returns the ConnectorId field if non-nil, zero value otherwise.

### GetConnectorIdOk

`func (o *TasksCursorCursorAllOfDataInner) GetConnectorIdOk() (*string, bool)`

GetConnectorIdOk returns a tuple with the ConnectorId field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetConnectorId

`func (o *TasksCursorCursorAllOfDataInner) SetConnectorId(v string)`

SetConnectorId sets ConnectorId field to given value.


### GetCreatedAt

`func (o *TasksCursorCursorAllOfDataInner) GetCreatedAt() time.Time`

GetCreatedAt returns the CreatedAt field if non-nil, zero value otherwise.

### GetCreatedAtOk

`func (o *TasksCursorCursorAllOfDataInner) GetCreatedAtOk() (*time.Time, bool)`

GetCreatedAtOk returns a tuple with the CreatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCreatedAt

`func (o *TasksCursorCursorAllOfDataInner) SetCreatedAt(v time.Time)`

SetCreatedAt sets CreatedAt field to given value.


### GetUpdatedAt

`func (o *TasksCursorCursorAllOfDataInner) GetUpdatedAt() time.Time`

GetUpdatedAt returns the UpdatedAt field if non-nil, zero value otherwise.

### GetUpdatedAtOk

`func (o *TasksCursorCursorAllOfDataInner) GetUpdatedAtOk() (*time.Time, bool)`

GetUpdatedAtOk returns a tuple with the UpdatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetUpdatedAt

`func (o *TasksCursorCursorAllOfDataInner) SetUpdatedAt(v time.Time)`

SetUpdatedAt sets UpdatedAt field to given value.


### GetStatus

`func (o *TasksCursorCursorAllOfDataInner) GetStatus() PaymentStatus`

GetStatus returns the Status field if non-nil, zero value otherwise.

### GetStatusOk

`func (o *TasksCursorCursorAllOfDataInner) GetStatusOk() (*PaymentStatus, bool)`

GetStatusOk returns a tuple with the Status field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStatus

`func (o *TasksCursorCursorAllOfDataInner) SetStatus(v PaymentStatus)`

SetStatus sets Status field to given value.


### GetState

`func (o *TasksCursorCursorAllOfDataInner) GetState() map[string]interface{}`

GetState returns the State field if non-nil, zero value otherwise.

### GetStateOk

`func (o *TasksCursorCursorAllOfDataInner) GetStateOk() (*map[string]interface{}, bool)`

GetStateOk returns a tuple with the State field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetState

`func (o *TasksCursorCursorAllOfDataInner) SetState(v map[string]interface{})`

SetState sets State field to given value.


### GetError

`func (o *TasksCursorCursorAllOfDataInner) GetError() string`

GetError returns the Error field if non-nil, zero value otherwise.

### GetErrorOk

`func (o *TasksCursorCursorAllOfDataInner) GetErrorOk() (*string, bool)`

GetErrorOk returns a tuple with the Error field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetError

`func (o *TasksCursorCursorAllOfDataInner) SetError(v string)`

SetError sets Error field to given value.

### HasError

`func (o *TasksCursorCursorAllOfDataInner) HasError() bool`

HasError returns a boolean if a field has been set.

### GetDescriptor

`func (o *TasksCursorCursorAllOfDataInner) GetDescriptor() TaskBankingCircleAllOfDescriptor`

GetDescriptor returns the Descriptor field if non-nil, zero value otherwise.

### GetDescriptorOk

`func (o *TasksCursorCursorAllOfDataInner) GetDescriptorOk() (*TaskBankingCircleAllOfDescriptor, bool)`

GetDescriptorOk returns a tuple with the Descriptor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetDescriptor

`func (o *TasksCursorCursorAllOfDataInner) SetDescriptor(v TaskBankingCircleAllOfDescriptor)`

SetDescriptor sets Descriptor field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


