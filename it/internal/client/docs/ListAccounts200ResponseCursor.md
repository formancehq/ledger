# ListAccounts200ResponseCursor

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**PageSize** | **int32** |  | 
**HasMore** | Pointer to **bool** |  | [optional] 
**Previous** | Pointer to **string** |  | [optional] 
**Next** | Pointer to **string** |  | [optional] 
**Data** | [**[]Account**](Account.md) |  | 

## Methods

### NewListAccounts200ResponseCursor

`func NewListAccounts200ResponseCursor(pageSize int32, data []Account, ) *ListAccounts200ResponseCursor`

NewListAccounts200ResponseCursor instantiates a new ListAccounts200ResponseCursor object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewListAccounts200ResponseCursorWithDefaults

`func NewListAccounts200ResponseCursorWithDefaults() *ListAccounts200ResponseCursor`

NewListAccounts200ResponseCursorWithDefaults instantiates a new ListAccounts200ResponseCursor object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetPageSize

`func (o *ListAccounts200ResponseCursor) GetPageSize() int32`

GetPageSize returns the PageSize field if non-nil, zero value otherwise.

### GetPageSizeOk

`func (o *ListAccounts200ResponseCursor) GetPageSizeOk() (*int32, bool)`

GetPageSizeOk returns a tuple with the PageSize field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPageSize

`func (o *ListAccounts200ResponseCursor) SetPageSize(v int32)`

SetPageSize sets PageSize field to given value.


### GetHasMore

`func (o *ListAccounts200ResponseCursor) GetHasMore() bool`

GetHasMore returns the HasMore field if non-nil, zero value otherwise.

### GetHasMoreOk

`func (o *ListAccounts200ResponseCursor) GetHasMoreOk() (*bool, bool)`

GetHasMoreOk returns a tuple with the HasMore field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetHasMore

`func (o *ListAccounts200ResponseCursor) SetHasMore(v bool)`

SetHasMore sets HasMore field to given value.

### HasHasMore

`func (o *ListAccounts200ResponseCursor) HasHasMore() bool`

HasHasMore returns a boolean if a field has been set.

### GetPrevious

`func (o *ListAccounts200ResponseCursor) GetPrevious() string`

GetPrevious returns the Previous field if non-nil, zero value otherwise.

### GetPreviousOk

`func (o *ListAccounts200ResponseCursor) GetPreviousOk() (*string, bool)`

GetPreviousOk returns a tuple with the Previous field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetPrevious

`func (o *ListAccounts200ResponseCursor) SetPrevious(v string)`

SetPrevious sets Previous field to given value.

### HasPrevious

`func (o *ListAccounts200ResponseCursor) HasPrevious() bool`

HasPrevious returns a boolean if a field has been set.

### GetNext

`func (o *ListAccounts200ResponseCursor) GetNext() string`

GetNext returns the Next field if non-nil, zero value otherwise.

### GetNextOk

`func (o *ListAccounts200ResponseCursor) GetNextOk() (*string, bool)`

GetNextOk returns a tuple with the Next field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetNext

`func (o *ListAccounts200ResponseCursor) SetNext(v string)`

SetNext sets Next field to given value.

### HasNext

`func (o *ListAccounts200ResponseCursor) HasNext() bool`

HasNext returns a boolean if a field has been set.

### GetData

`func (o *ListAccounts200ResponseCursor) GetData() []Account`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *ListAccounts200ResponseCursor) GetDataOk() (*[]Account, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *ListAccounts200ResponseCursor) SetData(v []Account)`

SetData sets Data field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


