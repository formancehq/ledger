# ListWorkflowsResponseCursor

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PageSize** | **int64** |  | 
**HasMore** | Pointer to **bool** |  | [optional] 
**Previous** | Pointer to **string** |  | [optional] 
**Next** | Pointer to **string** |  | [optional] 
**Data** | [**[]Workflow**](Workflow.md) |  | 

## Methods

### NewListWorkflowsResponseCursor

`func NewListWorkflowsResponseCursor(pageSize int64, data []Workflow, ) *ListWorkflowsResponseCursor`

NewListWorkflowsResponseCursor instantiates a new ListWorkflowsResponseCursor object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewListWorkflowsResponseCursorWithDefaults

`func NewListWorkflowsResponseCursorWithDefaults() *ListWorkflowsResponseCursor`

NewListWorkflowsResponseCursorWithDefaults instantiates a new ListWorkflowsResponseCursor object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPageSize

`func (o *ListWorkflowsResponseCursor) GetPageSize() int64`

GetPageSize returns the PageSize field if non-nil, zero value otherwise.

### GetPageSizeOk

`func (o *ListWorkflowsResponseCursor) GetPageSizeOk() (*int64, bool)`

GetPageSizeOk returns a tuple with the PageSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPageSize

`func (o *ListWorkflowsResponseCursor) SetPageSize(v int64)`

SetPageSize sets PageSize field to given value.


### GetHasMore

`func (o *ListWorkflowsResponseCursor) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *ListWorkflowsResponseCursor) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *ListWorkflowsResponseCursor) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *ListWorkflowsResponseCursor) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetPrevious

`func (o *ListWorkflowsResponseCursor) GetPrevious() string`

GetPrevious returns the Previous field if non-nil, zero value otherwise.

### GetPreviousOk

`func (o *ListWorkflowsResponseCursor) GetPreviousOk() (*string, bool)`

GetPreviousOk returns a tuple with the Previous field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPrevious

`func (o *ListWorkflowsResponseCursor) SetPrevious(v string)`

SetPrevious sets Previous field to given value.

### HasPrevious

`func (o *ListWorkflowsResponseCursor) HasPrevious() bool`

HasPrevious returns a boolean if a field has been set.

### GetNext

`func (o *ListWorkflowsResponseCursor) GetNext() string`

GetNext returns the Next field if non-nil, zero value otherwise.

### GetNextOk

`func (o *ListWorkflowsResponseCursor) GetNextOk() (*string, bool)`

GetNextOk returns a tuple with the Next field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNext

`func (o *ListWorkflowsResponseCursor) SetNext(v string)`

SetNext sets Next field to given value.

### HasNext

`func (o *ListWorkflowsResponseCursor) HasNext() bool`

HasNext returns a boolean if a field has been set.

### GetData

`func (o *ListWorkflowsResponseCursor) GetData() []Workflow`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *ListWorkflowsResponseCursor) GetDataOk() (*[]Workflow, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *ListWorkflowsResponseCursor) SetData(v []Workflow)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


