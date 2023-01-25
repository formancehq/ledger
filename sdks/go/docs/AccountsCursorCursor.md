# AccountsCursorCursor

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PageSize** | **int64** |  | 
**HasMore** | **bool** |  | 
**Previous** | Pointer to **string** |  | [optional] 
**Next** | Pointer to **string** |  | [optional] 
**Data** | [**[]PaymentsAccount**](PaymentsAccount.md) |  | 

## Methods

### NewAccountsCursorCursor

`func NewAccountsCursorCursor(pageSize int64, hasMore bool, data []PaymentsAccount, ) *AccountsCursorCursor`

NewAccountsCursorCursor instantiates a new AccountsCursorCursor object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewAccountsCursorCursorWithDefaults

`func NewAccountsCursorCursorWithDefaults() *AccountsCursorCursor`

NewAccountsCursorCursorWithDefaults instantiates a new AccountsCursorCursor object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPageSize

`func (o *AccountsCursorCursor) GetPageSize() int64`

GetPageSize returns the PageSize field if non-nil, zero value otherwise.

### GetPageSizeOk

`func (o *AccountsCursorCursor) GetPageSizeOk() (*int64, bool)`

GetPageSizeOk returns a tuple with the PageSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPageSize

`func (o *AccountsCursorCursor) SetPageSize(v int64)`

SetPageSize sets PageSize field to given value.


### GetHasMore

`func (o *AccountsCursorCursor) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *AccountsCursorCursor) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *AccountsCursorCursor) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.


### GetPrevious

`func (o *AccountsCursorCursor) GetPrevious() string`

GetPrevious returns the Previous field if non-nil, zero value otherwise.

### GetPreviousOk

`func (o *AccountsCursorCursor) GetPreviousOk() (*string, bool)`

GetPreviousOk returns a tuple with the Previous field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPrevious

`func (o *AccountsCursorCursor) SetPrevious(v string)`

SetPrevious sets Previous field to given value.

### HasPrevious

`func (o *AccountsCursorCursor) HasPrevious() bool`

HasPrevious returns a boolean if a field has been set.

### GetNext

`func (o *AccountsCursorCursor) GetNext() string`

GetNext returns the Next field if non-nil, zero value otherwise.

### GetNextOk

`func (o *AccountsCursorCursor) GetNextOk() (*string, bool)`

GetNextOk returns a tuple with the Next field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNext

`func (o *AccountsCursorCursor) SetNext(v string)`

SetNext sets Next field to given value.

### HasNext

`func (o *AccountsCursorCursor) HasNext() bool`

HasNext returns a boolean if a field has been set.

### GetData

`func (o *AccountsCursorCursor) GetData() []PaymentsAccount`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *AccountsCursorCursor) GetDataOk() (*[]PaymentsAccount, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *AccountsCursorCursor) SetData(v []PaymentsAccount)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


