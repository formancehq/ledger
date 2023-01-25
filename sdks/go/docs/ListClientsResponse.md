# ListClientsResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**Data** | Pointer to [**[]Client**](Client.md) |  | [optional] 

## Methods

### NewListClientsResponse

`func NewListClientsResponse() *ListClientsResponse`

NewListClientsResponse instantiates a new ListClientsResponse object
This constructor will assign default values to properties that have it defined,
and makes sure properties required by API are set, but the set of arguments
will change when the set of required properties is changed

### NewListClientsResponseWithDefaults

`func NewListClientsResponseWithDefaults() *ListClientsResponse`

NewListClientsResponseWithDefaults instantiates a new ListClientsResponse object
This constructor will only assign default values to properties that have it defined,
but it doesn't guarantee that properties required by API are set

### GetData

`func (o *ListClientsResponse) GetData() []Client`

GetData returns the Data field if non-nil, zero value otherwise.

### GetDataOk

`func (o *ListClientsResponse) GetDataOk() (*[]Client, bool)`

GetDataOk returns a tuple with the Data field if it's non-nil, zero value otherwise
and a boolean to check if the value has been set.

### SetData

`func (o *ListClientsResponse) SetData(v []Client)`

SetData sets Data field to given value.

### HasData

`func (o *ListClientsResponse) HasData() bool`

HasData returns a boolean if a field has been set.


[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


