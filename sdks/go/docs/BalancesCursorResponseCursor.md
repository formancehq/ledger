# BalancesCursorResponseCursor

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PageSize** | **int64** |  | 
**HasMore** | **bool** |  | 
**Previous** | Pointer to **string** |  | [optional] 
**Next** | Pointer to **string** |  | [optional] 
**Data** | [**[]map[string]map[string]int64**](map[string]map[string]int64.md) |  | 

## Methods

### NewBalancesCursorResponseCursor

`func NewBalancesCursorResponseCursor(pageSize int64, hasMore bool, data []map[string]map[string]int64, ) *BalancesCursorResponseCursor`

NewBalancesCursorResponseCursor instantiates a new BalancesCursorResponseCursor object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewBalancesCursorResponseCursorWithDefaults

`func NewBalancesCursorResponseCursorWithDefaults() *BalancesCursorResponseCursor`

NewBalancesCursorResponseCursorWithDefaults instantiates a new BalancesCursorResponseCursor object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPageSize

`func (o *BalancesCursorResponseCursor) GetPageSize() int64`

GetPageSize returns the PageSize field if non-nil, zero value otherwise.

### GetPageSizeOk

`func (o *BalancesCursorResponseCursor) GetPageSizeOk() (*int64, bool)`

GetPageSizeOk returns a tuple with the PageSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPageSize

`func (o *BalancesCursorResponseCursor) SetPageSize(v int64)`

SetPageSize sets PageSize field to given value.


### GetHasMore

`func (o *BalancesCursorResponseCursor) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *BalancesCursorResponseCursor) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *BalancesCursorResponseCursor) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.


### GetPrevious

`func (o *BalancesCursorResponseCursor) GetPrevious() string`

GetPrevious returns the Previous field if non-nil, zero value otherwise.

### GetPreviousOk

`func (o *BalancesCursorResponseCursor) GetPreviousOk() (*string, bool)`

GetPreviousOk returns a tuple with the Previous field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPrevious

`func (o *BalancesCursorResponseCursor) SetPrevious(v string)`

SetPrevious sets Previous field to given value.

### HasPrevious

`func (o *BalancesCursorResponseCursor) HasPrevious() bool`

HasPrevious returns a boolean if a field has been set.

### GetNext

`func (o *BalancesCursorResponseCursor) GetNext() string`

GetNext returns the Next field if non-nil, zero value otherwise.

### GetNextOk

`func (o *BalancesCursorResponseCursor) GetNextOk() (*string, bool)`

GetNextOk returns a tuple with the Next field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNext

`func (o *BalancesCursorResponseCursor) SetNext(v string)`

SetNext sets Next field to given value.

### HasNext

`func (o *BalancesCursorResponseCursor) HasNext() bool`

HasNext returns a boolean if a field has been set.

### GetData

`func (o *BalancesCursorResponseCursor) GetData() []map[string]map[string]int64`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *BalancesCursorResponseCursor) GetDataOk() (*[]map[string]map[string]int64, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *BalancesCursorResponseCursor) SetData(v []map[string]map[string]int64)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


