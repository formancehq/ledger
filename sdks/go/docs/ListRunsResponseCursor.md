# ListRunsResponseCursor

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PageSize** | **int64** |  | 
**HasMore** | Pointer to **bool** |  | [optional] 
**Previous** | Pointer to **string** |  | [optional] 
**Next** | Pointer to **string** |  | [optional] 
**Data** | [**[]WorkflowOccurrence**](WorkflowOccurrence.md) |  | 

## Methods

### NewListRunsResponseCursor

`func NewListRunsResponseCursor(pageSize int64, data []WorkflowOccurrence, ) *ListRunsResponseCursor`

NewListRunsResponseCursor instantiates a new ListRunsResponseCursor object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewListRunsResponseCursorWithDefaults

`func NewListRunsResponseCursorWithDefaults() *ListRunsResponseCursor`

NewListRunsResponseCursorWithDefaults instantiates a new ListRunsResponseCursor object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPageSize

`func (o *ListRunsResponseCursor) GetPageSize() int64`

GetPageSize returns the PageSize field if non-nil, zero value otherwise.

### GetPageSizeOk

`func (o *ListRunsResponseCursor) GetPageSizeOk() (*int64, bool)`

GetPageSizeOk returns a tuple with the PageSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPageSize

`func (o *ListRunsResponseCursor) SetPageSize(v int64)`

SetPageSize sets PageSize field to given value.


### GetHasMore

`func (o *ListRunsResponseCursor) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *ListRunsResponseCursor) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *ListRunsResponseCursor) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *ListRunsResponseCursor) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetPrevious

`func (o *ListRunsResponseCursor) GetPrevious() string`

GetPrevious returns the Previous field if non-nil, zero value otherwise.

### GetPreviousOk

`func (o *ListRunsResponseCursor) GetPreviousOk() (*string, bool)`

GetPreviousOk returns a tuple with the Previous field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPrevious

`func (o *ListRunsResponseCursor) SetPrevious(v string)`

SetPrevious sets Previous field to given value.

### HasPrevious

`func (o *ListRunsResponseCursor) HasPrevious() bool`

HasPrevious returns a boolean if a field has been set.

### GetNext

`func (o *ListRunsResponseCursor) GetNext() string`

GetNext returns the Next field if non-nil, zero value otherwise.

### GetNextOk

`func (o *ListRunsResponseCursor) GetNextOk() (*string, bool)`

GetNextOk returns a tuple with the Next field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNext

`func (o *ListRunsResponseCursor) SetNext(v string)`

SetNext sets Next field to given value.

### HasNext

`func (o *ListRunsResponseCursor) HasNext() bool`

HasNext returns a boolean if a field has been set.

### GetData

`func (o *ListRunsResponseCursor) GetData() []WorkflowOccurrence`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *ListRunsResponseCursor) GetDataOk() (*[]WorkflowOccurrence, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *ListRunsResponseCursor) SetData(v []WorkflowOccurrence)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


