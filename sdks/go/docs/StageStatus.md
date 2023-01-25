# StageStatus

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Stage** | **float32** |  | 
**OccurrenceID** | **string** |  | 
**StartedAt** | **time.Time** |  | 
**TerminatedAt** | Pointer to **time.Time** |  | [optional] 
**Error** | Pointer to **string** |  | [optional] 

## Methods

### NewStageStatus

`func NewStageStatus(stage float32, occurrenceID string, startedAt time.Time, ) *StageStatus`

NewStageStatus instantiates a new StageStatus object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewStageStatusWithDefaults

`func NewStageStatusWithDefaults() *StageStatus`

NewStageStatusWithDefaults instantiates a new StageStatus object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetStage

`func (o *StageStatus) GetStage() float32`

GetStage returns the Stage field if non-nil, zero value otherwise.

### GetStageOk

`func (o *StageStatus) GetStageOk() (*float32, bool)`

GetStageOk returns a tuple with the Stage field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStage

`func (o *StageStatus) SetStage(v float32)`

SetStage sets Stage field to given value.


### GetOccurrenceID

`func (o *StageStatus) GetOccurrenceID() string`

GetOccurrenceID returns the OccurrenceID field if non-nil, zero value otherwise.

### GetOccurrenceIDOk

`func (o *StageStatus) GetOccurrenceIDOk() (*string, bool)`

GetOccurrenceIDOk returns a tuple with the OccurrenceID field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetOccurrenceID

`func (o *StageStatus) SetOccurrenceID(v string)`

SetOccurrenceID sets OccurrenceID field to given value.


### GetStartedAt

`func (o *StageStatus) GetStartedAt() time.Time`

GetStartedAt returns the StartedAt field if non-nil, zero value otherwise.

### GetStartedAtOk

`func (o *StageStatus) GetStartedAtOk() (*time.Time, bool)`

GetStartedAtOk returns a tuple with the StartedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetStartedAt

`func (o *StageStatus) SetStartedAt(v time.Time)`

SetStartedAt sets StartedAt field to given value.


### GetTerminatedAt

`func (o *StageStatus) GetTerminatedAt() time.Time`

GetTerminatedAt returns the TerminatedAt field if non-nil, zero value otherwise.

### GetTerminatedAtOk

`func (o *StageStatus) GetTerminatedAtOk() (*time.Time, bool)`

GetTerminatedAtOk returns a tuple with the TerminatedAt field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTerminatedAt

`func (o *StageStatus) SetTerminatedAt(v time.Time)`

SetTerminatedAt sets TerminatedAt field to given value.

### HasTerminatedAt

`func (o *StageStatus) HasTerminatedAt() bool`

HasTerminatedAt returns a boolean if a field has been set.

### GetError

`func (o *StageStatus) GetError() string`

GetError returns the Error field if non-nil, zero value otherwise.

### GetErrorOk

`func (o *StageStatus) GetErrorOk() (*string, bool)`

GetErrorOk returns a tuple with the Error field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetError

`func (o *StageStatus) SetError(v string)`

SetError sets Error field to given value.

### HasError

`func (o *StageStatus) HasError() bool`

HasError returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


