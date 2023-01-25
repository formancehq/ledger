# GetTransactionsResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Cursor** | [**GetTransactionsResponseCursor**](GetTransactionsResponseCursor.md) |  | 

## Methods

### NewGetTransactionsResponse

`func NewGetTransactionsResponse(cursor GetTransactionsResponseCursor, ) *GetTransactionsResponse`

NewGetTransactionsResponse instantiates a new GetTransactionsResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewGetTransactionsResponseWithDefaults

`func NewGetTransactionsResponseWithDefaults() *GetTransactionsResponse`

NewGetTransactionsResponseWithDefaults instantiates a new GetTransactionsResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetCursor

`func (o *GetTransactionsResponse) GetCursor() GetTransactionsResponseCursor`

GetCursor returns the Cursor field if non-nil, zero value otherwise.

### GetCursorOk

`func (o *GetTransactionsResponse) GetCursorOk() (*GetTransactionsResponseCursor, bool)`

GetCursorOk returns a tuple with the Cursor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCursor

`func (o *GetTransactionsResponse) SetCursor(v GetTransactionsResponseCursor)`

SetCursor sets Cursor field to given value.



[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


