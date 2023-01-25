# Cursor

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PageSize** | Pointer to **int64** |  | [optional] 
**HasMore** | Pointer to **bool** |  | [optional] 
**Total** | Pointer to [**Total**](Total.md) |  | [optional] 
**Next** | Pointer to **string** |  | [optional] 
**Previous** | Pointer to **string** |  | [optional] 
**Data** | Pointer to **[]interface{}** |  | [optional] 

## Methods

### NewCursor

`func NewCursor() *Cursor`

NewCursor instantiates a new Cursor object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewCursorWithDefaults

`func NewCursorWithDefaults() *Cursor`

NewCursorWithDefaults instantiates a new Cursor object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPageSize

`func (o *Cursor) GetPageSize() int64`

GetPageSize returns the PageSize field if non-nil, zero value otherwise.

### GetPageSizeOk

`func (o *Cursor) GetPageSizeOk() (*int64, bool)`

GetPageSizeOk returns a tuple with the PageSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPageSize

`func (o *Cursor) SetPageSize(v int64)`

SetPageSize sets PageSize field to given value.

### HasPageSize

`func (o *Cursor) HasPageSize() bool`

HasPageSize returns a boolean if a field has been set.

### GetHasMore

`func (o *Cursor) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *Cursor) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *Cursor) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *Cursor) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetTotal

`func (o *Cursor) GetTotal() Total`

GetTotal returns the Total field if non-nil, zero value otherwise.

### GetTotalOk

`func (o *Cursor) GetTotalOk() (*Total, bool)`

GetTotalOk returns a tuple with the Total field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetTotal

`func (o *Cursor) SetTotal(v Total)`

SetTotal sets Total field to given value.

### HasTotal

`func (o *Cursor) HasTotal() bool`

HasTotal returns a boolean if a field has been set.

### GetNext

`func (o *Cursor) GetNext() string`

GetNext returns the Next field if non-nil, zero value otherwise.

### GetNextOk

`func (o *Cursor) GetNextOk() (*string, bool)`

GetNextOk returns a tuple with the Next field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNext

`func (o *Cursor) SetNext(v string)`

SetNext sets Next field to given value.

### HasNext

`func (o *Cursor) HasNext() bool`

HasNext returns a boolean if a field has been set.

### GetPrevious

`func (o *Cursor) GetPrevious() string`

GetPrevious returns the Previous field if non-nil, zero value otherwise.

### GetPreviousOk

`func (o *Cursor) GetPreviousOk() (*string, bool)`

GetPreviousOk returns a tuple with the Previous field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPrevious

`func (o *Cursor) SetPrevious(v string)`

SetPrevious sets Previous field to given value.

### HasPrevious

`func (o *Cursor) HasPrevious() bool`

HasPrevious returns a boolean if a field has been set.

### GetData

`func (o *Cursor) GetData() []interface{}`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *Cursor) GetDataOk() (*[]interface{}, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *Cursor) SetData(v []interface{})`

SetData sets Data field to given value.

### HasData

`func (o *Cursor) HasData() bool`

HasData returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


