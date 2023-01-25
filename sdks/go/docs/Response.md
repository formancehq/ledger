# Response

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Data** | Pointer to **map[string]interface{}** | The payload | [optional] 
**Cursor** | Pointer to [**Cursor**](Cursor.md) |  | [optional] 

## Methods

### NewResponse

`func NewResponse() *Response`

NewResponse instantiates a new Response object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewResponseWithDefaults

`func NewResponseWithDefaults() *Response`

NewResponseWithDefaults instantiates a new Response object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetData

`func (o *Response) GetData() map[string]interface{}`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *Response) GetDataOk() (*map[string]interface{}, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *Response) SetData(v map[string]interface{})`

SetData sets Data field to given value.

### HasData

`func (o *Response) HasData() bool`

HasData returns a boolean if a field has been set.

### GetCursor

`func (o *Response) GetCursor() Cursor`

GetCursor returns the Cursor field if non-nil, zero value otherwise.

### GetCursorOk

`func (o *Response) GetCursorOk() (*Cursor, bool)`

GetCursorOk returns a tuple with the Cursor field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetCursor

`func (o *Response) SetCursor(v Cursor)`

SetCursor sets Cursor field to given value.

### HasCursor

`func (o *Response) HasCursor() bool`

HasCursor returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


