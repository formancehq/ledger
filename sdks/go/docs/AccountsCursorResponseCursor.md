# AccountsCursorResponseCursor

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PageSize** | **int64** |  | 
**HasMore** | **bool** |  | 
**Previous** | Pointer to **string** |  | [optional] 
**Next** | Pointer to **string** |  | [optional] 
**Data** | [**[]Account**](Account.md) |  | 

## Methods

### NewAccountsCursorResponseCursor

`func NewAccountsCursorResponseCursor(pageSize int64, hasMore bool, data []Account, ) *AccountsCursorResponseCursor`

NewAccountsCursorResponseCursor instantiates a new AccountsCursorResponseCursor object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewAccountsCursorResponseCursorWithDefaults

`func NewAccountsCursorResponseCursorWithDefaults() *AccountsCursorResponseCursor`

NewAccountsCursorResponseCursorWithDefaults instantiates a new AccountsCursorResponseCursor object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPageSize

`func (o *AccountsCursorResponseCursor) GetPageSize() int64`

GetPageSize returns the PageSize field if non-nil, zero value otherwise.

### GetPageSizeOk

`func (o *AccountsCursorResponseCursor) GetPageSizeOk() (*int64, bool)`

GetPageSizeOk returns a tuple with the PageSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPageSize

`func (o *AccountsCursorResponseCursor) SetPageSize(v int64)`

SetPageSize sets PageSize field to given value.


### GetHasMore

`func (o *AccountsCursorResponseCursor) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *AccountsCursorResponseCursor) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *AccountsCursorResponseCursor) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.


### GetPrevious

`func (o *AccountsCursorResponseCursor) GetPrevious() string`

GetPrevious returns the Previous field if non-nil, zero value otherwise.

### GetPreviousOk

`func (o *AccountsCursorResponseCursor) GetPreviousOk() (*string, bool)`

GetPreviousOk returns a tuple with the Previous field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPrevious

`func (o *AccountsCursorResponseCursor) SetPrevious(v string)`

SetPrevious sets Previous field to given value.

### HasPrevious

`func (o *AccountsCursorResponseCursor) HasPrevious() bool`

HasPrevious returns a boolean if a field has been set.

### GetNext

`func (o *AccountsCursorResponseCursor) GetNext() string`

GetNext returns the Next field if non-nil, zero value otherwise.

### GetNextOk

`func (o *AccountsCursorResponseCursor) GetNextOk() (*string, bool)`

GetNextOk returns a tuple with the Next field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNext

`func (o *AccountsCursorResponseCursor) SetNext(v string)`

SetNext sets Next field to given value.

### HasNext

`func (o *AccountsCursorResponseCursor) HasNext() bool`

HasNext returns a boolean if a field has been set.

### GetData

`func (o *AccountsCursorResponseCursor) GetData() []Account`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *AccountsCursorResponseCursor) GetDataOk() (*[]Account, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *AccountsCursorResponseCursor) SetData(v []Account)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


